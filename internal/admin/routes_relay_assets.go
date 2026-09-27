package admin

import (
	"errors"
	"io"
	"mime/multipart"
	"net/http"
	"os"
	"strconv"
	"sync"

	"github.com/michmich112/congee/internal/config"
	"github.com/michmich112/congee/internal/relay"
	"github.com/michmich112/congee/internal/storage"
	"github.com/rs/zerolog"
)

func handleGetRelayAsset(cfgPath, asset string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		raw, err := os.ReadFile(cfgPath)
		if err != nil {
			http.Error(w, `{"error":"read config failed"}`, http.StatusInternalServerError)
			return
		}
		cfg, err := config.ParseConfigJSON(raw)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		data, contentType, err := relay.ReadNIP11Preview(cfgPath, cfg.NIP11, asset)
		if err != nil {
			if errors.Is(err, relay.ErrNIP11ImageExternal) || os.IsNotExist(err) {
				http.NotFound(w, r)
				return
			}
			http.NotFound(w, r)
			return
		}
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Type", contentType)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(data)
	}
}

func handlePostRelayAsset(cfgPath string, cfgMu *sync.Mutex, st storage.Store, log zerolog.Logger, scheduleRestart func(), asset string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		maxBytes := config.NIP11IconMaxBytes
		if asset == config.NIP11AssetBanner {
			maxBytes = config.NIP11BannerMaxBytes
		}
		r.Body = http.MaxBytesReader(w, r.Body, int64(maxBytes)+4096)
		data, err := readRelayAssetFile(r, maxBytes)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		if err := config.ValidateNIP11Upload(asset, data); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}

		cfgMu.Lock()
		defer cfgMu.Unlock()

		prev, err := os.ReadFile(cfgPath)
		if err != nil {
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "read config failed"})
			return
		}
		cfg, err := config.ParseConfigJSON(prev)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		prevSource := cfg.NIP11.ImageSource(asset)
		if err := config.WriteNIP11Asset(cfgPath, asset, data); err != nil {
			log.Warn().Err(err).Str("asset", asset).Msg("nip11 upload write failed")
			writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "write image failed"})
			return
		}
		setNIP11Upload(cfg, asset)
		if err := config.WriteConfigAtomic(cfgPath, cfg); err != nil {
			if prevSource != config.NIP11ImageSourceUpload {
				if path, pathErr := config.NIP11AssetFile(cfgPath, asset); pathErr == nil {
					_ = os.Remove(path)
				}
			}
			writeJSON(w, http.StatusBadRequest, map[string]string{"error": err.Error()})
			return
		}
		diff := "previous_bytes=" + strconv.Itoa(len(prev)) + "\nnip11 " + asset + " source=upload"
		if err := config.SaveConfigChange(r.Context(), st, "POST /api/relay-assets/"+asset, diff); err != nil {
			http.Error(w, `{"error":"changelog write failed"}`, http.StatusInternalServerError)
			return
		}
		needRestart := configRestartNeeded(prev, cfg)
		if needRestart && scheduleRestart != nil {
			go scheduleRestartSoon(scheduleRestart)
		}
		writeJSON(w, http.StatusOK, map[string]any{
			"ok":               true,
			"restart_required": needRestart,
			"restarting":       needRestart && scheduleRestart != nil,
		})
	}
}

func setNIP11Upload(cfg *config.Config, asset string) {
	switch asset {
	case config.NIP11AssetBanner:
		cfg.NIP11.BannerSource = config.NIP11ImageSourceUpload
		cfg.NIP11.Banner = ""
	default:
		cfg.NIP11.IconSource = config.NIP11ImageSourceUpload
		cfg.NIP11.Icon = ""
	}
}

func readRelayAssetFile(r *http.Request, maxBytes int) ([]byte, error) {
	if err := r.ParseMultipartForm(int64(maxBytes) + 1024); err != nil {
		return nil, err
	}
	file, _, err := r.FormFile("file")
	if err != nil {
		return nil, errors.New("file is required")
	}
	defer file.Close()
	return readLimited(file, maxBytes)
}

func readLimited(file multipart.File, maxBytes int) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(file, int64(maxBytes)+1))
	if err != nil {
		return nil, err
	}
	if len(data) > maxBytes {
		return nil, errors.New("image exceeds size cap")
	}
	return data, nil
}
