package main

import (
	"context"
	"database/sql"
	"errors"
	"net/http"
	"sort"
	"strconv"

	"github.com/jmoiron/sqlx"
	"github.com/labstack/echo/v4"
)

type LivestreamStatistics struct {
	Rank           int64 `json:"rank"`
	ViewersCount   int64 `json:"viewers_count"`
	TotalReactions int64 `json:"total_reactions"`
	TotalReports   int64 `json:"total_reports"`
	MaxTip         int64 `json:"max_tip"`
}

type LivestreamRankingEntry struct {
	LivestreamID int64
	Score        int64
}
type LivestreamRanking []LivestreamRankingEntry

func (r LivestreamRanking) Len() int      { return len(r) }
func (r LivestreamRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r LivestreamRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].LivestreamID < r[j].LivestreamID
	} else {
		return r[i].Score < r[j].Score
	}
}

type UserStatistics struct {
	Rank              int64  `json:"rank"`
	ViewersCount      int64  `json:"viewers_count"`
	TotalReactions    int64  `json:"total_reactions"`
	TotalLivecomments int64  `json:"total_livecomments"`
	TotalTip          int64  `json:"total_tip"`
	FavoriteEmoji     string `json:"favorite_emoji"`
}

type UserRankingEntry struct {
	Username string
	Score    int64
}
type UserRanking []UserRankingEntry

func (r UserRanking) Len() int      { return len(r) }
func (r UserRanking) Swap(i, j int) { r[i], r[j] = r[j], r[i] }
func (r UserRanking) Less(i, j int) bool {
	if r[i].Score == r[j].Score {
		return r[i].Username < r[j].Username
	} else {
		return r[i].Score < r[j].Score
	}
}

func getUserStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		// echo.NewHTTPErrorが返っているのでそのまま出力
		return err
	}

	username := c.Param("username")
	// ユーザごとに、紐づく配信について、累計リアクション数、累計ライブコメント数、累計売上金額を算出
	// また、現在の合計視聴者数もだす

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	user, err := getUserByName(ctx, tx, username)
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusBadRequest, "not found user that has the given username")
	}
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get user: "+err.Error())
	}

	// ランク算出
	var ranking UserRanking
	type TmpReactionTip struct {
		TotalReactions int64  `db:"total_reactions"`
		TotalTips      int64  `db:"total_tips"`
		UserName       string `db:"user_name"`
	}
	var tmpReactionTips []*TmpReactionTip
	query := `
	SELECT COUNT(reactions.id) AS total_reactions, IFNULL(SUM(livecomments.tip), 0) AS total_tips, users.name AS user_name
	FROM users
	LEFT JOIN livestreams ON livestreams.user_id = users.id
	LEFT JOIN reactions ON reactions.livestream_id = livestreams.id
	LEFT JOIN livecomments ON livecomments.livestream_id = livestreams.id
	GROUP BY users.id FOR SHARE`
	if err := tx.SelectContext(ctx, &tmpReactionTips, query); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count reactions and tips: "+err.Error())
	}

	for _, tmp := range tmpReactionTips {
		score := tmp.TotalReactions + tmp.TotalTips
		ranking = append(ranking, UserRankingEntry{
			Username: tmp.UserName,
			Score:    score,
		})
	}

	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.Username == username {
			break
		}
		rank++
	}

	// リアクション数
	var totalReactions int64
	query = `SELECT COUNT(1) FROM users u 
    INNER JOIN livestreams l ON l.user_id = u.id 
    INNER JOIN reactions r ON r.livestream_id = l.id
    WHERE u.name = ?
	`
	if err := tx.GetContext(ctx, &totalReactions, query, username); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total reactions: "+err.Error())
	}

	// ライブコメント数、チップ合計
	var totalLivecomments int64
	var totalTip int64
	var livestreams []*LivestreamModel
	if err := tx.SelectContext(ctx, &livestreams, "SELECT * FROM livestreams WHERE user_id = ?", user.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	type TmpLiveStatistic struct {
		TotalTips         int64 `db:"total_tip"`
		TotalLivecomments int64 `db:"total_live_comments"`
	}
	var tmpLiveStatistic TmpLiveStatistic
	if err := tx.GetContext(ctx, &tmpLiveStatistic, `
SELECT SUM(tip) AS total_tip, COUNT(1) AS total_live_comments
FROM livecomments
INNER JOIN livestreams ON livecomments.livestream_id = livestreams.id
WHERE livestreams.user_id = ?
GROUP BY livestreams.user_id
`, user.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livecomments: "+err.Error())
	}
	totalLivecomments = tmpLiveStatistic.TotalLivecomments
	totalTip = tmpLiveStatistic.TotalTips

	// 合計視聴者数
	var viewersCount int64
	for _, livestream := range livestreams {
		var cnt int64
		if err := tx.GetContext(ctx, &cnt, "SELECT COUNT(1) FROM livestream_viewers_history WHERE livestream_id = ?", livestream.ID); err != nil && !errors.Is(err, sql.ErrNoRows) {
			return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream_view_history: "+err.Error())
		}
		viewersCount += cnt
	}

	// お気に入り絵文字
	var favoriteEmoji string
	query = `
	SELECT r.emoji_name
	FROM users u
	INNER JOIN livestreams l ON l.user_id = u.id
	INNER JOIN reactions r ON r.livestream_id = l.id
	WHERE u.name = ?
	GROUP BY emoji_name
	ORDER BY COUNT(1) DESC, emoji_name DESC
	LIMIT 1
	`
	if err := tx.GetContext(ctx, &favoriteEmoji, query, username); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find favorite emoji: "+err.Error())
	}

	stats := UserStatistics{
		Rank:              rank,
		ViewersCount:      viewersCount,
		TotalReactions:    totalReactions,
		TotalLivecomments: totalLivecomments,
		TotalTip:          totalTip,
		FavoriteEmoji:     favoriteEmoji,
	}
	return c.JSON(http.StatusOK, stats)
}

func getLivestreamStatisticsHandler(c echo.Context) error {
	ctx := c.Request().Context()

	if err := verifyUserSession(c); err != nil {
		return err
	}

	id, err := strconv.Atoi(c.Param("livestream_id"))
	if err != nil {
		return echo.NewHTTPError(http.StatusBadRequest, "livestream_id in path must be integer")
	}
	livestreamID := int64(id)

	tx, err := dbConn.BeginTxx(ctx, nil)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to begin transaction: "+err.Error())
	}
	defer tx.Rollback()

	_, err = getLivestream(ctx, tx, int(livestreamID))
	if errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusBadRequest, "not found livestream that has the given id")
	} else if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestream: "+err.Error())
	}

	var livestreams []struct {
		ID int64 `db:"id"`
	}
	if err := tx.SelectContext(ctx, &livestreams, "SELECT id FROM livestreams"); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	data := map[int64]LivestreamRankingEntry{}
	for _, s := range livestreams {
		data[s.ID] = LivestreamRankingEntry{LivestreamID: s.ID}
	}

	// Reactionの集計
	type LiveReaction struct {
		ID        int64 `db:"id"`
		Reactions int64 `db:"reactions"`
	}
	var reactions []*LiveReaction
	if err := tx.SelectContext(ctx, &reactions, `
SELECT
    r.livestream_id AS id,
    COUNT(*) AS reactions
FROM reactions r
GROUP BY r.livestream_id
`); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	// チップの集計
	type LiveTotalTip struct {
		ID        int64 `db:"id"`
		TotalTips int64 `db:"total_tips"`
	}
	var tips []*LiveTotalTip
	if err := tx.SelectContext(ctx, &tips, `
SELECT
    l2.livestream_id AS id,
    IFNULL(SUM(l2.tip), 0) AS total_tips
FROM livecomments l2
GROUP BY l2.livestream_id
`); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to get livestreams: "+err.Error())
	}

	// NOTE: ハッシュの値を更新するときは一回外に出してからやる
	for _, r := range reactions {
		d := data[r.ID]
		d.Score += r.Reactions
		data[r.ID] = d
	}
	for _, t := range tips {
		d := data[t.ID]
		d.Score += t.TotalTips
		data[t.ID] = d
	}

	var ranking = make(LivestreamRanking, 0, len(data))
	for _, entry := range data {
		ranking = append(ranking, entry)
	}

	sort.Sort(ranking)

	var rank int64 = 1
	for i := len(ranking) - 1; i >= 0; i-- {
		entry := ranking[i]
		if entry.LivestreamID == livestreamID {
			break
		}
		rank++
	}

	// 視聴者数算出
	viewersCount, err := calcViewerCount(tx, ctx, livestreamID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count livestream viewers: "+err.Error())
	}

	// 最大チップ額
	maxTip, err := calcMaxTip(tx, ctx, livestreamID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to find maximum tip livecomment: "+err.Error())
	}

	// リアクション数
	var totalReactions int64
	if err := tx.GetContext(ctx, &totalReactions, "SELECT COUNT(1) FROM livestreams l INNER JOIN reactions r ON r.livestream_id = l.id WHERE l.id = ?", livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total reactions: "+err.Error())
	}

	// スパム報告数
	totalReports, err := calcTotalReports(tx, ctx, livestreamID)
	if err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to count total spam reports: "+err.Error())
	}

	if err := tx.Commit(); err != nil {
		return echo.NewHTTPError(http.StatusInternalServerError, "failed to commit: "+err.Error())
	}

	return c.JSON(http.StatusOK, LivestreamStatistics{
		Rank:           rank,
		ViewersCount:   viewersCount,
		MaxTip:         maxTip,
		TotalReactions: totalReactions,
		TotalReports:   totalReports,
	})
}

// スパム報告数
func calcTotalReports(tx *sqlx.Tx, ctx context.Context, livestreamID int64) (int64, error) {
	var totalReports int64
	if err := tx.GetContext(ctx, &totalReports, `SELECT COUNT(1) FROM livecomment_reports r WHERE r.livestream_id = ? GROUP BY r.livestream_id`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return totalReports, nil
}

// 最大チップ額
func calcMaxTip(tx *sqlx.Tx, ctx context.Context, livestreamID int64) (int64, error) {
	var maxTip int64
	if err := tx.GetContext(ctx, &maxTip, `SELECT IFNULL(MAX(tip), 0) FROM livecomments l2 WHERE l2.livestream_id = ? GROUP BY l2.livestream_id`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return maxTip, nil
}

// 視聴者数算出
func calcViewerCount(tx *sqlx.Tx, ctx context.Context, livestreamID int64) (int64, error) {
	var viewersCount int64
	if err := tx.GetContext(ctx, &viewersCount, `SELECT COUNT(1) FROM livestream_viewers_history h WHERE h.livestream_id = ? GROUP BY h.livestream_id`, livestreamID); err != nil && !errors.Is(err, sql.ErrNoRows) {
		return 0, err
	}
	return viewersCount, nil
}
