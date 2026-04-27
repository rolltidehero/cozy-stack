package nextcloud

import (
	"context"
	"errors"
	"fmt"
	"path"
	"time"

	"github.com/cozy/cozy-stack/model/instance"
	"github.com/cozy/cozy-stack/pkg/consts"
	"github.com/cozy/cozy-stack/pkg/couchdb"
	"github.com/cozy/cozy-stack/pkg/logger"
	"github.com/cozy/cozy-stack/pkg/webdav"
)

// ErrMigrationNotDeletable gates delete-source to the completed status.
// Failed and canceled migrations may still hold files on Nextcloud that
// never reached Cozy, and removing the folder would lose them.
var ErrMigrationNotDeletable = errors.New("nextcloud migration source can only be deleted after a completed migration")

// ErrMigrationSourceAlreadyDeleted is surfaced rather than swallowed so
// duplicate clicks from the UI show up as bugs instead of silent no-ops.
var ErrMigrationSourceAlreadyDeleted = errors.New("nextcloud migration source already deleted")

// ErrNextcloudAccountMissing means the single nextcloud account was
// removed out-of-band between the migration and this cleanup call.
var ErrNextcloudAccountMissing = errors.New("no nextcloud account configured for this instance")

// DeleteMigrationSource removes the Nextcloud content ingested by the
// given migration, then empties the user's trashbin so the migrated
// bytes stop counting against quota.
//
// SourcePath == "/" enumerates the WebDAV home and deletes each
// top-level child: Nextcloud refuses DELETE on the home itself with
// 403, so a single call would fail. An empty SourcePath is treated as
// "/" to match legacy docs predating source_path persistence.
//
// Error contract (priority order, for errors.Is mapping):
//
//   - [ErrMigrationNotFound], [ErrMigrationNotDeletable],
//     [ErrMigrationSourceAlreadyDeleted],
//     [ErrNextcloudAccountMissing]: state gates.
//   - [webdav.ErrInvalidAuth]: stored credentials rejected (401/403).
//   - [ErrNextcloudUnreachable]: any other Nextcloud-side failure.
func DeleteMigrationSource(
	ctx context.Context,
	inst *instance.Instance,
	migrationID string,
) error {
	log := logger.FromContext(ctx)

	var doc Migration
	if err := couchdb.GetDoc(inst, consts.NextcloudMigrations, migrationID, &doc); err != nil {
		if couchdb.IsNotFoundError(err) || couchdb.IsNoDatabaseError(err) {
			return ErrMigrationNotFound
		}
		return fmt.Errorf("load migration %s: %w", migrationID, err)
	}
	if doc.Status != MigrationStatusCompleted {
		log.WithField("status", doc.Status).
			Infof("Delete-source rejected: migration is not completed")
		return ErrMigrationNotDeletable
	}
	if doc.SourceDeletedAt != nil {
		log.Infof("Delete-source rejected: source already deleted")
		return ErrMigrationSourceAlreadyDeleted
	}
	sourcePath := doc.SourcePath
	if sourcePath == "" {
		sourcePath = "/"
	}

	acc, err := FindNextcloudAccount(inst)
	if err != nil {
		return fmt.Errorf("find nextcloud account: %w", err)
	}
	if acc == nil {
		return ErrNextcloudAccountMissing
	}

	nc, err := newFromAccountDoc(inst, acc)
	if err != nil {
		if errors.Is(err, webdav.ErrInvalidAuth) {
			return err
		}
		return fmt.Errorf("build nextcloud client: %w", err)
	}

	log = log.WithField("source_path", sourcePath)
	if sourcePath == "/" {
		if err := deleteHomeContents(nc, log); err != nil {
			return err
		}
	} else {
		if err := deleteSinglePath(nc, sourcePath, log); err != nil {
			return err
		}
	}
	if err := emptyNextcloudTrash(nc, log); err != nil {
		return err
	}

	now := time.Now().UTC()
	doc.SourceDeletedAt = &now
	if err := couchdb.UpdateDoc(inst, &doc); err != nil {
		return fmt.Errorf("stamp source_deleted_at: %w", err)
	}
	return nil
}

func deleteSinglePath(nc *NextCloud, sourcePath string, log logger.Logger) error {
	switch err := nc.Delete(sourcePath); {
	case err == nil:
		log.Infof("Nextcloud migration source folder deleted")
		return nil
	case errors.Is(err, webdav.ErrNotFound):
		log.Warnf("Nextcloud migration source folder was already gone")
		return nil
	case errors.Is(err, webdav.ErrInvalidAuth):
		return err
	default:
		log.Warnf("Nextcloud delete failed: %s", err)
		return fmt.Errorf("%w: %w", ErrNextcloudUnreachable, err)
	}
}

func deleteHomeContents(nc *NextCloud, log logger.Logger) error {
	items, err := nc.webdav.List("/files/" + nc.userID + "/")
	switch {
	case err == nil:
	case errors.Is(err, webdav.ErrInvalidAuth):
		return err
	case errors.Is(err, webdav.ErrNotFound):
		log.Warnf("Nextcloud home was already gone")
		return nil
	default:
		log.Warnf("Nextcloud list home failed: %s", err)
		return fmt.Errorf("%w: list home: %w", ErrNextcloudUnreachable, err)
	}
	for _, item := range items {
		name := path.Base(item.Href)
		childLog := log.WithField("child", name)
		switch err := nc.Delete(name); {
		case err == nil:
			childLog.Infof("Nextcloud home child deleted")
		case errors.Is(err, webdav.ErrNotFound):
			childLog.Warnf("Nextcloud home child was already gone")
		case errors.Is(err, webdav.ErrInvalidAuth):
			return err
		default:
			childLog.Warnf("Nextcloud delete failed: %s", err)
			return fmt.Errorf("%w: delete %q: %w", ErrNextcloudUnreachable, name, err)
		}
	}
	return nil
}

func emptyNextcloudTrash(nc *NextCloud, log logger.Logger) error {
	switch err := nc.EmptyTrash(); {
	case err == nil:
		log.Infof("Nextcloud trash emptied")
		return nil
	case errors.Is(err, webdav.ErrNotFound):
		log.Infof("Nextcloud trash was already empty")
		return nil
	case errors.Is(err, webdav.ErrInvalidAuth):
		return err
	default:
		log.Warnf("Nextcloud trash empty failed: %s", err)
		return fmt.Errorf("%w: empty trash: %w", ErrNextcloudUnreachable, err)
	}
}
