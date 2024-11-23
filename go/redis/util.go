package redis

import (
	"context"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/jmoiron/sqlx"
)

type redisRepository[T any] struct {
	db    *sqlx.DB
	cache *Cache[T]
}

func NewRedisRepository[T any](
	db *sqlx.DB,
	cacheClient Client,
) *redisRepository[T] {
	return &redisRepository[T]{
		db: db,
		cache: NewCache[T](
			cacheClient,
			time.Minute,
		),
	}
}

func (r *redisRepository[T]) GetByColumn(
	ctx context.Context,
	columnName string, // 検索するカラム名 (e.g., "id", "name")
	columnValue string, // 検索値 (e.g., idの値, nameの値)
	tableName string,
	columns ...string, // 取得するカラム（オプション）
) (T, error) {
	cacheKey := fmt.Sprintf("%s:%s:%s", tableName, columnName, columnValue) // columnNameとcolumnValueでキャッシュキーを生成

	return r.cache.GetOrSet(
		ctx, cacheKey, func(ctx context.Context) (T, error) {
			var result T
			dest := any(&result)

			selectColumns := "*"
			if len(columns) > 0 {
				selectColumns = strings.Join(columns, ", ")
			}

			query := fmt.Sprintf("SELECT %s FROM `%s` WHERE %s = ?", selectColumns, tableName, columnName)

			// columnValueをintに変換可能か確認
			intValue, err := strconv.Atoi(columnValue)
			if err == nil {
				// 変換成功
				columnValue = intValue
			}

			if err := r.db.GetContext(ctx, dest, query, columnValue); err != nil {
				return result, err
			}

			return result, nil
		},
	)
}

func (r *redisRepository[T]) GetById(
	ctx context.Context,
	id string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "id", id, tableName, columns...)
}

func (r *redisRepository[T]) GetByName(
	ctx context.Context,
	name string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "name", name, tableName, columns...)
}

func (r *redisRepository[T]) GetByUserId(
	ctx context.Context,
	userId string,
	tableName string,
	columns ...string,
) (T, error) {
	return r.GetByColumn(ctx, "user_id", userId, tableName, columns...)
}
