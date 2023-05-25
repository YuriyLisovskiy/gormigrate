package gormigrate

import (
	stderrors "errors"
	"fmt"
	"github.com/pkg/errors"

	"gorm.io/gorm"
	"gorm.io/gorm/clause"
)

const lockID = "lock"

var ErrAlreadyLocked = errors.New("migration table is already locked")

type migrationLock struct {
	ID       string `gorm:"column:id;primary_key"`
	IsLocked bool   `gorm:"column:is_locked"`
}

func acquireLock(db *gorm.DB) error {
	lock := new(migrationLock)

	err := db.Model(lock).
		Where("id = ?", lockID).
		First(lock).
		Error

	switch {
	case stderrors.Is(err, gorm.ErrRecordNotFound):
		lock.IsLocked = true
		if db.Create(&lock).Error != nil {
			return fmt.Errorf("create lock entity: %w", err)
		}
	case err != nil:
		return fmt.Errorf("get lock entity: %w", err)
	default:
		if lock.IsLocked {
			return ErrAlreadyLocked
		}

		lock.IsLocked = true
		err = updateLock(db, lock)
		if err != nil {
			return fmt.Errorf("update lock: %w", err)
		}
	}

	return nil
}

func releaseLock(db *gorm.DB) error {
	lock := &migrationLock{ID: lockID, IsLocked: false}

	return errors.WithMessage(updateLock(db, lock), "update lock")
}

func updateLock(db *gorm.DB, lock *migrationLock) error {
	tx := db.
		Session(&gorm.Session{}).
		Model(lock).
		Clauses(clause.Returning{}).
		Where("id = ?", lockID).
		Updates(lock)

	return errors.WithMessage(tx.Error, "update entity")
}
