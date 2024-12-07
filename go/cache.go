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
