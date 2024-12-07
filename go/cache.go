package main

import "context"

type db interface {
	GetContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
	SelectContext(ctx context.Context, dest interface{}, query string, args ...interface{}) error
}

func getUserById(ctx context.Context, tx db, userId int64) (*UserModel, error) {
	ownerModel := UserModel{}
	cachedUserModel, ok := usersCache.Load(userId)
	if ok {
		ownerModel = cachedUserModel.(UserModel)
	} else {
		err := tx.GetContext(ctx, &ownerModel, "SELECT * FROM users WHERE id = ?", userId)
		if err != nil {
			return nil, err
		}
		usersCache.Store(userId, ownerModel)
	}
	return &ownerModel, nil
}

func getUserByName(ctx context.Context, tx db, name string) (*UserModel, error) {
	ownerModel := UserModel{}
	cachedUserModel, ok := usersByNameCache.Load(name)
	if ok {
		ownerModel = cachedUserModel.(UserModel)
	} else {
		err := tx.GetContext(ctx, &ownerModel, "SELECT * FROM users WHERE name = ?", name)
		if err != nil {
			return nil, err
		}
		usersByNameCache.Store(name, ownerModel)
	}
	return &ownerModel, nil
}

func getThemeByUserId(ctx context.Context, tx db, userId int64) (*ThemeModel, error) {
	var themeModel ThemeModel
	cachedThemeModel, ok := themeModelCache.Load(userId)
	if ok {
		themeModel = cachedThemeModel.(ThemeModel)
	} else {
		err := tx.GetContext(ctx, &themeModel, "SELECT * FROM themes WHERE user_id = ?", userId)
		if err != nil {
			return nil, err
		}
		themeModelCache.Store(userId, themeModel)
	}
	return &themeModel, nil
}

func getTagById(ctx context.Context, tx db, tagId int64) (*TagModel, error) {
	tagModel := TagModel{}
	cachedTagModel, ok := tagsCache.Load(tagId)
	if ok {
		tagModel = cachedTagModel.(TagModel)
	} else {
		var tags []TagModel
		err := tx.SelectContext(ctx, &tags, "SELECT * FROM tags WHERE id = ?", tagId)
		if err != nil {
			return nil, err
		}
		for _, tag := range tags {
			tagsCache.Store(tag.ID, tag)
		}
		tagModel = tags[0]
	}
	return &tagModel, nil
}

func getLivestream(ctx context.Context, tx db, livestreamID int) (*LivestreamModel, error) {
	livestreamModel := LivestreamModel{}
	livestream, ok := livestreamCache.Load(livestreamID)
	if ok {
		livestreamModel = livestream.(LivestreamModel)
	} else {
		err := tx.GetContext(ctx, &livestreamModel, "SELECT * FROM livestreams WHERE id = ?", livestreamID)
		if err != nil {
			return nil, err
		}
		livestreamCache.Store(livestreamID, livestreamModel)
	}

	return &livestreamModel, nil
}
