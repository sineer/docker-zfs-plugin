package zfsdriver

import (
	"fmt"
	"strings"
	"time"

	"github.com/clinta/go-zfs"
	"github.com/docker/go-plugins-helpers/volume"
	log "github.com/sirupsen/logrus"
)

//ZfsDriver implements the plugin helpers volume.Driver interface for zfs
type ZfsDriver struct {
	volume.Driver
	rds []*zfs.Dataset //root dataset
}

//NewZfsDriver returns the plugin driver object
func NewZfsDriver(dss ...string) (*ZfsDriver, error) {
	log.Debug("Creating new ZfsDriver.")
	zd := &ZfsDriver{}
	if len(dss) < 1 {
		return nil, fmt.Errorf("No datasets specified")
	}
	for _, ds := range dss {
		if !zfs.DatasetExists(ds) {
			_, err := zfs.CreateDatasetRecursive(ds, make(map[string]string))
			if err != nil {
				log.Error("Failed to create root dataset.")
				return nil, err
			}
		}
		rds, err := zfs.GetDataset(ds)
		if err != nil {
			log.Error("Failed to get root dataset.")
			return nil, err
		}
		zd.rds = append(zd.rds, rds)
	}

	return zd, nil
}

//Create creates a new zfs dataset for a volume
func (zd *ZfsDriver) Create(req *volume.CreateRequest) error {
	log.WithField("Request", req).Debug("Create")

	// Parse the volume name to extract project name if it exists
	// Docker Compose volumes are named: projectname_volumename
	volumeName := req.Name
	datasetName := volumeName
	
	// Check if this looks like a docker-compose volume (contains underscore)
	if strings.Contains(volumeName, "_") {
		parts := strings.SplitN(volumeName, "_", 2)
		if len(parts) == 2 {
			// Assume first part is project name, second is actual volume name
			projectName := parts[0]
			actualVolumeName := parts[1]
			
			// Create hierarchical structure for docker-compose projects
			// This allows efficient recursive snapshots per project
			if len(zd.rds) > 0 {
				// Use the first root dataset as base
				rootDS := zd.rds[0].Name
				datasetName = fmt.Sprintf("%s/%s/%s", rootDS, projectName, actualVolumeName)
				log.WithFields(log.Fields{
					"project": projectName,
					"volume": actualVolumeName,
					"dataset": datasetName,
				}).Info("Creating hierarchical dataset for docker-compose volume")
			}
		}
	}

	if zfs.DatasetExists(datasetName) {
		return fmt.Errorf("volume already exists: %s", datasetName)
	}

	// CreateDatasetRecursive will create parent datasets if needed
	_, err := zfs.CreateDatasetRecursive(datasetName, req.Options)
	if err != nil {
		return fmt.Errorf("failed to create dataset %s: %w", datasetName, err)
	}
	
	log.WithField("dataset", datasetName).Info("Successfully created hierarchical dataset")
	return nil
}

//List returns a list of zfs volumes on this host
func (zd *ZfsDriver) List() (*volume.ListResponse, error) {
	log.Debug("List")
	var vols []*volume.Volume

	for _, rds := range zd.rds {
		dsl, err := rds.DatasetList()
		if err != nil {
			return nil, err
		}
		for _, ds := range dsl {
			//TODO: rewrite this to utilize zd.getVolume() when
			//upstream go-zfs is rewritten to cache properties
			var mp string
			mp, err = ds.GetMountpoint()
			if err != nil {
				log.WithField("name", ds.Name).Error("Failed to get mountpoint from dataset")
				continue
			}
			vols = append(vols, &volume.Volume{Name: ds.Name, Mountpoint: mp})
		}
	}

	return &volume.ListResponse{Volumes: vols}, nil
}

//Get returns the volume.Volume{} object for the requested volume
//nolint: dupl
func (zd *ZfsDriver) Get(req *volume.GetRequest) (*volume.GetResponse, error) {
	log.WithField("Request", req).Debug("Get")

	v, err := zd.getVolume(req.Name)
	if err != nil {
		return nil, err
	}

	return &volume.GetResponse{Volume: v}, nil
}

func (zd *ZfsDriver) getVolume(name string) (*volume.Volume, error) {
	ds, err := zfs.GetDataset(name)
	if err != nil {
		return nil, err
	}

	mp, err := ds.GetMountpoint()
	if err != nil {
		return nil, err
	}

	ts, err := ds.GetCreation()
	if err != nil {
		log.WithError(err).Error("Failed to get creation property from zfs dataset")
		return &volume.Volume{Name: name, Mountpoint: mp}, nil
	}

	return &volume.Volume{Name: name, Mountpoint: mp, CreatedAt: ts.Format(time.RFC3339)}, nil
}

func (zd *ZfsDriver) getMP(name string) (string, error) {
	ds, err := zfs.GetDataset(name)
	if err != nil {
		return "", err
	}

	return ds.GetMountpoint()
}

//Remove destroys a zfs dataset for a volume
func (zd *ZfsDriver) Remove(req *volume.RemoveRequest) error {
	log.WithField("Request", req).Debug("Remove")

	ds, err := zfs.GetDataset(req.Name)
	if err != nil {
		return err
	}

	return ds.Destroy()
}

//Path returns the mountpoint of a volume
//nolint: dupl
func (zd *ZfsDriver) Path(req *volume.PathRequest) (*volume.PathResponse, error) {
	log.WithField("Request", req).Debug("Path")

	mp, err := zd.getMP(req.Name)
	if err != nil {
		return nil, err
	}

	return &volume.PathResponse{Mountpoint: mp}, nil
}

//Mount returns the mountpoint of the zfs volume
//nolint: dupl
func (zd *ZfsDriver) Mount(req *volume.MountRequest) (*volume.MountResponse, error) {
	log.WithField("Request", req).Debug("Mount")
	mp, err := zd.getMP(req.Name)
	if err != nil {
		return nil, err
	}

	return &volume.MountResponse{Mountpoint: mp}, nil
}

//Unmount does nothing because a zfs dataset need not be unmounted
func (zd *ZfsDriver) Unmount(req *volume.UnmountRequest) error {
	log.WithField("Request", req).Debug("Unmount")
	return nil
}

//Capabilities sets the scope to local as this is a local only driver
func (zd *ZfsDriver) Capabilities() *volume.CapabilitiesResponse {
	log.Debug("Capabilities")
	return &volume.CapabilitiesResponse{Capabilities: volume.Capability{Scope: "local"}}
}
