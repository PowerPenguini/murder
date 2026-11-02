package reconcile

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/PowerPenguini/murder/internal/runtime"
	"github.com/PowerPenguini/murder/internal/spec"
)

func (r *Reconciler) image(ctx context.Context, image string) error {
	present, err := r.runtime.HasImage(ctx, image)
	if err != nil {
		return fmt.Errorf("inspect image: %w", err)
	}
	if present {
		r.pulls.present(image)
		return nil
	}
	return r.pulls.fetch(image)
}

// imageRetry is how long an image the engine kept, in use by a container,
// waits before the next attempt to remove it.
const imageRetry = time.Hour

// sweepImages removes images no tenant needs any more, so old releases do
// not fill the disk. A tenant needs the image it runs, the one its data has
// and the one before (a rollback then needs no registry). Only images
// pinned by registry digest go: those can be pulled again, a local image
// could not. Images are compared by ID, so an image a tenant needs under
// another name stays.
func (r *Reconciler) sweepImages(ctx context.Context) {
	needed := map[string]bool{}
	for _, record := range r.own() {
		for _, image := range []string{record.Spec.Image, record.DataImage, record.PreviousImage} {
			if image != "" {
				needed[image] = true
			}
		}
	}
	now := time.Now()
	var candidates []string
	for _, image := range r.store.Images() {
		switch {
		case needed[image] || now.Before(r.imageRetryAt[image]):
		case !spec.RegistryPinned(image):
			r.forgetImage(image) // a local image stays on the node, untracked
		default:
			candidates = append(candidates, image)
		}
	}
	if len(candidates) == 0 {
		return
	}
	neededIDs := map[string]bool{}
	for image := range needed {
		id, err := r.runtime.ImageID(ctx, image)
		if err != nil {
			r.log.Warn("inspect an image a tenant needs; no image is removed this pass", "image", image, "error", err)
			return
		}
		neededIDs[id] = true
	}
	for _, image := range candidates {
		id, err := r.runtime.ImageID(ctx, image)
		switch {
		case err != nil:
			r.log.Warn("inspect an image no tenant needs", "image", image, "error", err)
			continue
		case id == "" && r.pulls.pulling(image):
			continue // its pull may still bring it
		case id == "":
			r.forgetImage(image) // gone already
			continue
		case neededIDs[id]:
			r.imageRetryAt[image] = now.Add(imageRetry) // needed under another name
			continue
		case r.needed(image):
			continue // a tenant was given it since the sweep began
		}
		switch err := r.runtime.RemoveImage(ctx, id); {
		case errors.Is(err, runtime.ErrInUse):
			r.imageRetryAt[image] = now.Add(imageRetry)
		case err != nil:
			r.log.Warn("remove an image no tenant needs", "image", image, "error", err)
			r.imageRetryAt[image] = now.Add(imageRetry)
		default:
			r.log.Info("removed an image no tenant needs", "image", image)
			r.forgetImage(image)
		}
	}
}

// needed reports whether a tenant needs image now, read from the store
// right before it is removed: API writes do not wait for the pass.
func (r *Reconciler) needed(image string) bool {
	for _, record := range r.own() {
		if record.Spec.Image == image || record.DataImage == image || record.PreviousImage == image {
			return true
		}
	}
	return false
}

func (r *Reconciler) forgetImage(image string) {
	delete(r.imageRetryAt, image)
	if err := r.store.ForgetImage(image); err != nil {
		r.log.Warn("forget an image", "image", image, "error", err)
	}
}
