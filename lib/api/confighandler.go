// Copyright (C) 2020 The Syncthing Authors.
//
// This Source Code Form is subject to the terms of the Mozilla Public
// License, v. 2.0. If a copy of the MPL was not distributed with this file,
// You can obtain one at https://mozilla.org/MPL/2.0/.

package api

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strings"

	"github.com/julienschmidt/httprouter"

	"github.com/syncthing/syncthing/internal/slogutil"
	"github.com/syncthing/syncthing/lib/config"
	"github.com/syncthing/syncthing/lib/fs"
	"github.com/syncthing/syncthing/lib/protocol"
	"github.com/syncthing/syncthing/lib/structutil"
)

type configMuxBuilder struct {
	*httprouter.Router

	id  protocol.DeviceID
	cfg config.Wrapper
}

func (c *configMuxBuilder) registerConfig(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.RawCopy())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustConfig(w, r)
	})
}

func (c *configMuxBuilder) registerConfigDeprecated(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.RawCopy())
	})

	c.HandlerFunc(http.MethodPost, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustConfig(w, r)
	})
}

func (c *configMuxBuilder) registerConfigInsync(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, map[string]bool{"configInSync": !c.cfg.RequiresRestart()})
	})
}

func (c *configMuxBuilder) registerConfigRequiresRestart(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, map[string]bool{"requiresRestart": c.cfg.RequiresRestart()})
	})
}

func (c *configMuxBuilder) registerFolders(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.FolderList())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		data, err := unmarshalToRawMessages(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		folders := make([]config.FolderConfiguration, len(data))
		defaultFolder := c.cfg.DefaultFolder()
		for i, bs := range data {
			folders[i] = defaultFolder.Copy()
			if err := json.Unmarshal(bs, &folders[i]); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
			cfg.SetFolders(folders)
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		c.finish(w, waiter)
	})

	c.HandlerFunc(http.MethodPost, path, func(w http.ResponseWriter, r *http.Request) {
		c.addFolder(w, r)
	})
}

func (c *configMuxBuilder) registerDevices(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.DeviceList())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		data, err := unmarshalToRawMessages(r.Body)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		devices := make([]config.DeviceConfiguration, len(data))
		defaultDevice := c.cfg.DefaultDevice()
		for i, bs := range data {
			devices[i] = defaultDevice.Copy()
			if err := json.Unmarshal(bs, &devices[i]); err != nil {
				http.Error(w, err.Error(), http.StatusBadRequest)
				return
			}
		}
		waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
			cfg.SetDevices(devices)
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		c.finish(w, waiter)
	})

	c.HandlerFunc(http.MethodPost, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustDevice(w, r, c.cfg.DefaultDevice(), false)
	})
}

func (c *configMuxBuilder) registerFolder(path string) {
	c.Handle(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
		folder, ok := c.cfg.Folder(p.ByName("id"))
		if !ok {
			http.Error(w, "No folder with given ID", http.StatusNotFound)
			return
		}
		sendJSON(w, folder)
	})

	c.Handle(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		c.addFolder(w, r)
	})

	c.Handle(http.MethodPatch, path, func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		folder, ok := c.cfg.Folder(p.ByName("id"))
		if !ok {
			http.Error(w, "No folder with given ID", http.StatusNotFound)
			return
		}
		c.adjustFolder(w, r, folder, false)
	})

	c.Handle(http.MethodDelete, path, func(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
		waiter, err := c.cfg.RemoveFolder(p.ByName("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		c.finish(w, waiter)
	})
}

func (c *configMuxBuilder) registerDevice(path string) {
	deviceFromParams := func(w http.ResponseWriter, p httprouter.Params) (config.DeviceConfiguration, bool) {
		id, err := protocol.DeviceIDFromString(p.ByName("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return config.DeviceConfiguration{}, false
		}
		device, ok := c.cfg.Device(id)
		if !ok {
			http.Error(w, "No device with given ID", http.StatusNotFound)
			return config.DeviceConfiguration{}, false
		}
		return device, true
	}

	c.Handle(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
		if device, ok := deviceFromParams(w, p); ok {
			sendJSON(w, device)
		}
	})

	c.Handle(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		c.adjustDevice(w, r, c.cfg.DefaultDevice(), false)
	})

	c.Handle(http.MethodPatch, path, func(w http.ResponseWriter, r *http.Request, p httprouter.Params) {
		if device, ok := deviceFromParams(w, p); ok {
			c.adjustDevice(w, r, device, false)
		}
	})

	c.Handle(http.MethodDelete, path, func(w http.ResponseWriter, _ *http.Request, p httprouter.Params) {
		id, err := protocol.DeviceIDFromString(p.ByName("id"))
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		waiter, err := c.cfg.RemoveDevice(id)
		if err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		c.finish(w, waiter)
	})
}

func (c *configMuxBuilder) registerDefaultFolder(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.DefaultFolder())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		var cfg config.FolderConfiguration
		structutil.SetDefaults(&cfg)
		c.adjustFolder(w, r, cfg, true)
	})

	c.HandlerFunc(http.MethodPatch, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustFolder(w, r, c.cfg.DefaultFolder(), true)
	})
}

func (c *configMuxBuilder) registerDefaultDevice(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.DefaultDevice())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		var cfg config.DeviceConfiguration
		structutil.SetDefaults(&cfg)
		c.adjustDevice(w, r, cfg, true)
	})

	c.HandlerFunc(http.MethodPatch, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustDevice(w, r, c.cfg.DefaultDevice(), true)
	})
}

func (c *configMuxBuilder) registerDefaultIgnores(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.DefaultIgnores())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		var ignores config.Ignores
		if err := unmarshalTo(r.Body, &ignores); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
			cfg.Defaults.Ignores = ignores
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		c.finish(w, waiter)
	})
}

func (c *configMuxBuilder) registerOptions(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.Options())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		var cfg config.OptionsConfiguration
		structutil.SetDefaults(&cfg)
		c.adjustOptions(w, r, cfg)
	})

	c.HandlerFunc(http.MethodPatch, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustOptions(w, r, c.cfg.Options())
	})
}

func (c *configMuxBuilder) registerLDAP(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.LDAP())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		var cfg config.LDAPConfiguration
		structutil.SetDefaults(&cfg)
		c.adjustLDAP(w, r, cfg)
	})

	c.HandlerFunc(http.MethodPatch, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustLDAP(w, r, c.cfg.LDAP())
	})
}

func (c *configMuxBuilder) registerGUI(path string) {
	c.HandlerFunc(http.MethodGet, path, func(w http.ResponseWriter, _ *http.Request) {
		sendJSON(w, c.cfg.GUI())
	})

	c.HandlerFunc(http.MethodPut, path, func(w http.ResponseWriter, r *http.Request) {
		var cfg config.GUIConfiguration
		structutil.SetDefaults(&cfg)
		c.adjustGUI(w, r, cfg)
	})

	c.HandlerFunc(http.MethodPatch, path, func(w http.ResponseWriter, r *http.Request) {
		c.adjustGUI(w, r, c.cfg.GUI())
	})
}

func (c *configMuxBuilder) adjustConfig(w http.ResponseWriter, r *http.Request) {
	to, err := config.ReadJSON(r.Body, c.id)
	r.Body.Close()
	if err != nil {
		slog.Error("Failed to decode posted config", slogutil.Error(err))
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var errMsg string
	var status int
	waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
		if err := c.postAdjustGui(&cfg.GUI, &to.GUI); err != nil {
			errMsg = err.Error()
			status = http.StatusInternalServerError
			return
		}
		*cfg = to
	})
	if errMsg != "" {
		http.Error(w, errMsg, status)
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.finish(w, waiter)
}

// addFolder adds a folder, or replaces the one with the same ID, starting
// from the folder defaults. A new folder's relative path is resolved against
// the default folder path.
func (c *configMuxBuilder) addFolder(w http.ResponseWriter, r *http.Request) {
	defaults := c.cfg.DefaultFolder()
	folder := defaults.Copy()
	if err := unmarshalTo(r.Body, &folder); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if _, exists := c.cfg.Folder(folder.ID); !exists && isRelativeFolderPath(folder.Path) {
		folder.Path = filepath.Join(defaults.Path, folder.Path)
		if status, err := checkNewFolderPath(folder); err != nil {
			http.Error(w, err.Error(), status)
			return
		}
	}
	c.setFolder(w, folder, false)
}

// isRelativeFolderPath reports whether path is neither absolute nor starts
// with a tilde, which is expanded to the home directory. A relative folder
// path is otherwise relative to the working directory of the process.
func isRelativeFolderPath(path string) bool {
	if path == "" || filepath.IsAbs(path) {
		return false
	}
	path = filepath.FromSlash(path)
	return path != "~" && !strings.HasPrefix(path, "~"+string(filepath.Separator))
}

// checkNewFolderPath refuses a path resolved for a new folder that holds
// something already: as it was derived from a name rather than given in
// full, it's unlikely to be the intended place for existing data.
func checkNewFolderPath(folder config.FolderConfiguration) (int, error) {
	ffs := fs.NewFilesystem(folder.FilesystemType.ToFS(), folder.Path)
	info, err := ffs.Lstat(".")
	if fs.IsNotExist(err) {
		return 0, nil
	}
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if !info.IsDir() {
		return http.StatusConflict, fmt.Errorf("%s exists and is not a directory", folder.Path)
	}
	names, err := ffs.DirNames(".")
	if err != nil {
		return http.StatusInternalServerError, err
	}
	if len(names) > 0 {
		return http.StatusConflict, fmt.Errorf("%s exists and is not empty", folder.Path)
	}
	return 0, nil
}

func (c *configMuxBuilder) adjustFolder(w http.ResponseWriter, r *http.Request, folder config.FolderConfiguration, defaults bool) {
	if err := unmarshalTo(r.Body, &folder); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	c.setFolder(w, folder, defaults)
}

func (c *configMuxBuilder) setFolder(w http.ResponseWriter, folder config.FolderConfiguration, defaults bool) {
	waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
		if defaults {
			cfg.Defaults.Folder = folder
		} else {
			cfg.SetFolder(folder)
		}
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.finish(w, waiter)
}

func (c *configMuxBuilder) adjustDevice(w http.ResponseWriter, r *http.Request, device config.DeviceConfiguration, defaults bool) {
	if err := unmarshalTo(r.Body, &device); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
		if defaults {
			cfg.Defaults.Device = device
		} else {
			cfg.SetDevice(device)
		}
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.finish(w, waiter)
}

func (c *configMuxBuilder) adjustOptions(w http.ResponseWriter, r *http.Request, opts config.OptionsConfiguration) {
	if err := unmarshalTo(r.Body, &opts); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
		cfg.Options = opts
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.finish(w, waiter)
}

func (c *configMuxBuilder) adjustGUI(w http.ResponseWriter, r *http.Request, gui config.GUIConfiguration) {
	err := unmarshalTo(r.Body, &gui)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	var errMsg string
	var status int
	waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
		if err := c.postAdjustGui(&cfg.GUI, &gui); err != nil {
			errMsg = err.Error()
			status = http.StatusInternalServerError
			return
		}
		cfg.GUI = gui
	})
	if errMsg != "" {
		http.Error(w, errMsg, status)
	} else if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.finish(w, waiter)
}

func (c *configMuxBuilder) postAdjustGui(from *config.GUIConfiguration, to *config.GUIConfiguration) error {
	if to.Password != from.Password {
		if err := to.SetPassword(to.Password); err != nil {
			slog.Error("Failed to hash password", slogutil.Error(err))
			return err
		}
	}
	return nil
}

func (c *configMuxBuilder) adjustLDAP(w http.ResponseWriter, r *http.Request, ldap config.LDAPConfiguration) {
	if err := unmarshalTo(r.Body, &ldap); err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	waiter, err := c.cfg.Modify(func(cfg *config.Configuration) {
		cfg.LDAP = ldap
	})
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	c.finish(w, waiter)
}

// Unmarshals the content of the given body and stores it in to (i.e. to must be a pointer).
func unmarshalTo(body io.ReadCloser, to any) error {
	bs, err := io.ReadAll(body)
	body.Close()
	if err != nil {
		return err
	}
	return json.Unmarshal(bs, to)
}

func unmarshalToRawMessages(body io.ReadCloser) ([]json.RawMessage, error) {
	var data []json.RawMessage
	err := unmarshalTo(body, &data)
	return data, err
}

func (c *configMuxBuilder) finish(w http.ResponseWriter, waiter config.Waiter) {
	waiter.Wait()
	if err := c.cfg.Save(); err != nil {
		slog.Error("Failed to save config", slogutil.Error(err))
		http.Error(w, err.Error(), http.StatusInternalServerError)
	}
}
