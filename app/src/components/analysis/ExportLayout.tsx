import { ReactNode } from 'react';

interface ExportLayoutProps {
  playerName?: string;
  teamName?: string;
  playerPhoto?: string;
  stat?: string;
  lineValue?: number;
  prediction?: string;
  aiPrediction?: number;
  hitRate?: number;
  children: ReactNode;
  theme: 'light' | 'dark';
  width: number;
  height: number;
}


export function ExportLayout({
  playerName,
  teamName,
  playerPhoto,
  stat,
  lineValue,
  prediction,
  aiPrediction,
  hitRate,
  children,
  theme,
  width,
  height,
}: ExportLayoutProps) {
  const isLightTheme = theme === 'light';

  return (
    <div
      className={`${isLightTheme ? 'light' : 'dark'}`}
      style={{
        width: `${width}px`,
        position: 'relative',
        fontFamily: 'Inter, system-ui, sans-serif',
        background: isLightTheme
          ? 'linear-gradient(135deg, #f5f7fa 0%, #c3cfe2 100%)'
          : 'linear-gradient(135deg, #1a1a2e 0%, #16213e 100%)',
      }}
    >

      <div
        style={{
          position: 'relative',
          padding: '20px 30px 30px 30px',
          display: 'flex',
          flexDirection: 'column',
          gap: '20px',
        }}
      >
        {playerName && (
          <div
            style={{
              display: 'flex',
              alignItems: 'center',
              gap: '20px',
              padding: '20px',
              background: isLightTheme
                ? 'rgba(255, 255, 255, 0.95)'
                : 'rgba(30, 30, 46, 0.95)',
              borderRadius: '16px',
              backdropFilter: 'blur(10px)',
              boxShadow: isLightTheme
                ? '0 4px 6px rgba(0, 0, 0, 0.1)'
                : '0 4px 6px rgba(0, 0, 0, 0.3)',
            }}
          >
            {playerPhoto && (
              <div style={{
                width: '110px',
                height: '110px',
                borderRadius: '12px',
                overflow: 'hidden',
                flexShrink: 0,
                border: `2px solid ${isLightTheme ? '#93c5fd' : '#3b82f6'}`,
                display: 'flex',
                alignItems: 'center',
                justifyContent: 'center',
                background: isLightTheme ? '#f3f4f6' : '#1f2937',
              }}>
                <img
                  src={playerPhoto}
                  alt={playerName}
                  crossOrigin="anonymous"
                  onError={(e) => {
                    
                    const img = e.currentTarget;
                    img.style.display = 'none';
                    const parent = img.parentElement;
                    if (parent && playerName) {
                      const initials = playerName.split(' ').map(n => n[0]).join('').slice(0, 2);
                      parent.innerHTML = `<div style="font-size: 36px; font-weight: 700; color: ${isLightTheme ? '#3b82f6' : '#93c5fd'};">${initials}</div>`;
                    }
                  }}
                  style={{
                    width: '100%',
                    height: '100%',
                    objectFit: 'cover',
                    objectPosition: 'top center',
                  }}
                />
              </div>
            )}

            <div style={{ flex: 1, display: 'flex', flexDirection: 'column', justifyContent: 'center' }}>
              <h1
                style={{
                  fontSize: '28px',
                  fontWeight: 700,
                  margin: 0,
                  marginBottom: '6px',
                  color: isLightTheme ? '#1f2937' : '#f9fafb',
                }}
              >
                {playerName}
              </h1>
              {teamName && (
                <p
                  style={{
                    fontSize: '15px',
                    margin: 0,
                    marginBottom: '10px',
                    color: isLightTheme ? '#6b7280' : '#9ca3af',
                  }}
                >
                  {teamName}
                </p>
              )}

              {stat && lineValue !== undefined && (
                <div style={{ display: 'flex', flexDirection: 'column', gap: '6px' }}>
                  <div style={{ display: 'flex', gap: '20px', alignItems: 'baseline' }}>
                    <div style={{ display: 'flex', alignItems: 'baseline', gap: '6px' }}>
                      <span style={{ fontSize: '13px', color: isLightTheme ? '#6b7280' : '#9ca3af', fontWeight: 500 }}>
                        Line:
                      </span>
                      <span style={{ fontSize: '16px', fontWeight: 700, color: isLightTheme ? '#1f2937' : '#f9fafb' }}>
                        {lineValue}
                      </span>
                      <span style={{ fontSize: '13px', color: isLightTheme ? '#6b7280' : '#9ca3af', fontWeight: 500 }}>
                        {stat}
                      </span>
                    </div>
                    {prediction && (
                      <div style={{ display: 'flex', alignItems: 'baseline', gap: '6px' }}>
                        <span style={{ fontSize: '13px', color: isLightTheme ? '#6b7280' : '#9ca3af', fontWeight: 500 }}>
                          Prediction:
                        </span>
                        <span style={{ fontSize: '15px', fontWeight: 700, color: isLightTheme ? '#667eea' : '#93c5fd' }}>
                          {prediction.toUpperCase()}
                        </span>
                      </div>
                    )}
                  </div>

                  <div style={{ display: 'flex', gap: '16px', alignItems: 'center', fontSize: '12px', color: isLightTheme ? '#6b7280' : '#9ca3af' }}>
                    {hitRate !== undefined && (
                      <div style={{ display: 'flex', gap: '4px', alignItems: 'baseline' }}>
                        <span>Hit Rate:</span>
                        <span style={{ fontWeight: 600, fontSize: '13px', color: isLightTheme ? '#1f2937' : '#f9fafb' }}>{hitRate}%</span>
                      </div>
                    )}
                    {aiPrediction !== undefined && (
                      <div style={{ display: 'flex', gap: '4px', alignItems: 'baseline' }}>
                        <span>AI Prediction:</span>
                        <span style={{ fontWeight: 600, fontSize: '13px', color: isLightTheme ? '#667eea' : '#93c5fd' }}>{aiPrediction.toFixed(1)}</span>
                      </div>
                    )}
                  </div>
                </div>
              )}
            </div>
          </div>
        )}

        <div
          style={{
            display: 'flex',
            flexDirection: 'column',
            gap: '20px',
          }}
        >
          {children}
        </div>

        <div
          style={{
            display: 'flex',
            justifyContent: 'space-between',
            alignItems: 'center',
            padding: '12px 20px',
            background: isLightTheme
              ? 'rgba(255, 255, 255, 0.9)'
              : 'rgba(30, 30, 46, 0.9)',
            borderRadius: '12px',
            backdropFilter: 'blur(10px)',
          }}
        >
          <div style={{ display: 'flex', alignItems: 'center', gap: '12px' }}>
            <img
              src="/courtvision.png"
              alt="CourtVision"
              style={{
                width: '32px',
                height: '32px',
                borderRadius: '8px',
                objectFit: 'contain',
              }}
            />
            <div>
              <p
                style={{
                  margin: 0,
                  fontSize: '16px',
                  fontWeight: 700,
                  color: isLightTheme ? '#1f2937' : '#f9fafb',
                }}
              >
                CourtVision
              </p>
              <p
                style={{
                  margin: 0,
                  fontSize: '12px',
                  color: isLightTheme ? '#6b7280' : '#9ca3af',
                }}
              >
                AI Performance Analytics
              </p>
            </div>
          </div>
          <div
            style={{
              fontSize: '12px',
              color: isLightTheme ? '#9ca3af' : '#6b7280',
            }}
          >
            {new Date().toLocaleDateString('en-US', {
              month: 'short',
              day: 'numeric',
              year: 'numeric',
            })}
          </div>
        </div>
      </div>
    </div>
  );
}
