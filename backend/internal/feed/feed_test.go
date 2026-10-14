package feed

import (
	"testing"
)

func TestIsAuthSuccess(t *testing.T) {
	tests := []struct {
		name        string
		payload     map[string]interface{}
		wantSuccess bool
		wantErr     bool
	}{
		{
			name:        "Case 1: status ok",
			payload:     map[string]interface{}{"status": "ok"},
			wantSuccess: true,
			wantErr:     false,
		},
		{
			name:        "Case 2: status error",
			payload:     map[string]interface{}{"status": "error", "message": "bad api key"},
			wantSuccess: false,
			wantErr:     true,
		},
		{
			name:        "Case 3: status failed",
			payload:     map[string]interface{}{"status": "failed", "message": "unauthorized"},
			wantSuccess: false,
			wantErr:     true,
		},
		{
			name:        "Case 4: event authenticated",
			payload:     map[string]interface{}{"event": "authenticated"},
			wantSuccess: true,
			wantErr:     false,
		},
		{
			name:        "Case 5: event error",
			payload:     map[string]interface{}{"event": "error", "message": "session conflict"},
			wantSuccess: false,
			wantErr:     true,
		},
		{
			name:        "Case 6: boolean success true",
			payload:     map[string]interface{}{"success": true},
			wantSuccess: true,
			wantErr:     false,
		},
		{
			name:        "Case 7: boolean success false",
			payload:     map[string]interface{}{"success": false, "message": "auth failure"},
			wantSuccess: false,
			wantErr:     true,
		},
		{
			name:        "Case 8: token provided",
			payload:     map[string]interface{}{"token": "auth_token_xyz_123"},
			wantSuccess: true,
			wantErr:     false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotSuccess, err := isAuthSuccess(tt.payload)
			if (err != nil) != tt.wantErr {
				t.Errorf("isAuthSuccess() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotSuccess != tt.wantSuccess {
				t.Errorf("isAuthSuccess() gotSuccess = %v, wantSuccess %v", gotSuccess, tt.wantSuccess)
			}
		})
	}
}
