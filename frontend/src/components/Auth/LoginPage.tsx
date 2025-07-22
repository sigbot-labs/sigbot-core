import React, { useState } from 'react';
import { Github, Loader, Sun, Moon, Server } from 'lucide-react';
import { apiServiceFacade, ApiType } from '../../services';
import { User } from '../../types';
import { useTheme } from '../../contexts/ThemeContext';
import { useLanguage } from '../../hooks/useLanguage';
import { useTranslation } from 'react-i18next';

interface LoginPageProps {
  onLogin: (user: User) => void | Promise<void>;
}

export const LoginPage: React.FC<LoginPageProps> = ({ onLogin }) => {
  const { isDark, theme, setTheme } = useTheme();
  const { language, setLanguage } = useLanguage();
  const { t } = useTranslation();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [apiMode, setApiMode] = useState<ApiType>(apiServiceFacade.getCurrentApiType());

  const handleApiModeChange = (mode: ApiType) => {
    setApiMode(mode);
    apiServiceFacade.switchService(mode);
  };

  const handleGoogleLogin = async () => {
    setLoading(true);
    setError(null);

    try {
      const result = await apiServiceFacade.getService().login('google');
      await Promise.resolve(onLogin(result.user as User));
    } catch (err) {
      setError(err instanceof Error ? err.message : 'Login failed');
    } finally {
      setLoading(false);
    }
  };

  return (
    <div className={`min-h-screen ${isDark ? 'bg-gray-900' : 'bg-gray-50'} flex items-center justify-center relative`}>
      {/* Theme, Language and API Mode Controls */}
      <div className="absolute top-6 right-6 flex items-center space-x-4">
        {/* API Mode Toggle */}
        <div className="flex items-center space-x-1">
          <div className={`px-3 py-2 text-xs rounded-lg transition-colors ${
            isDark ? 'bg-gray-800 text-gray-300' : 'bg-white text-gray-700 border border-gray-300'
          }`}>
            <div className="flex items-center space-x-1">
              <Server className="w-3 h-3" />
              <span className="text-xs">{t('settings.apiMode')}:</span>
            </div>
          </div>
          {(['MOCK', 'API'] as const).map(modeOption => (
            <button
              key={modeOption}
              onClick={() => handleApiModeChange(modeOption)}
              className={`px-2 py-2 text-xs rounded-lg transition-colors ${
                apiMode === modeOption
                  ? 'bg-blue-600 text-white'
                  : `${isDark ? 'bg-gray-800 text-gray-300 hover:bg-gray-700' : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300'}`
              }`}
              title={t(`settings.${modeOption === 'MOCK' ? 'mockMode' : 'httpMode'}`)}
            >
              {modeOption}
            </button>
          ))}
        </div>

        {/* Theme Toggle */}
        <div className="flex items-center space-x-1">
          {(['light', 'dark'] as const).map(themeOption => (
            <button
              key={themeOption}
              onClick={() => setTheme(themeOption)}
              className={`p-2 rounded-lg transition-colors ${
                theme === themeOption
                  ? 'bg-blue-600 text-white'
                  : `${isDark ? 'bg-gray-800 text-gray-300 hover:bg-gray-700' : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300'}`
              }`}
              title={t(`theme.${themeOption}`)}
            >
              {themeOption === 'light' ? <Sun className="w-4 h-4" /> : <Moon className="w-4 h-4" />}
            </button>
          ))}
        </div>

        {/* Language Toggle */}
        <div className="flex items-center space-x-1">
          {(['en', 'zh'] as const).map(langOption => (
            <button
              key={langOption}
              onClick={() => setLanguage(langOption)}
              className={`px-3 py-2 text-sm rounded-lg transition-colors ${
                language === langOption
                  ? 'bg-blue-600 text-white'
                  : `${isDark ? 'bg-gray-800 text-gray-300 hover:bg-gray-700' : 'bg-white text-gray-700 hover:bg-gray-100 border border-gray-300'}`
              }`}
            >
              {langOption === 'en' ? 'EN' : '中'}
            </button>
          ))}
        </div>
      </div>

      <div className="max-w-md w-full space-y-8 p-8">
        <div className="text-center">
          <h1 className="text-4xl font-bold text-blue-400 mb-2">Signal Trading</h1>
          <p className={`${isDark ? 'text-gray-400' : 'text-gray-600'} text-lg`}>Focus on AI Trading Platform</p>
          <p className={`${isDark ? 'text-gray-500' : 'text-gray-500'} text-sm mt-4`}>{t('auth.loginDescriptionGoogle')}</p>
        </div>

        <div className={`${isDark ? 'bg-gray-800' : 'bg-white border border-gray-200'} rounded-lg p-8 shadow-xl`}>
          <div className="space-y-6">
            {error && <div className="bg-red-900 border border-red-700 text-red-300 px-4 py-3 rounded">{error}</div>}

            {/* Login Buttons */}
            <div className="space-y-3">
              {/* Google Login Button */}
              <button
                onClick={handleGoogleLogin}
                disabled={loading}
                className={`w-full flex items-center justify-center space-x-3 px-6 py-3 bg-blue-600 hover:bg-blue-700 disabled:bg-blue-400 text-white rounded-lg transition-colors`}
              >
                {loading ? (
                  <Loader className="w-5 h-5 animate-spin" />
                ) : (
                  <svg className="w-5 h-5" viewBox="0 0 24 24" fill="currentColor">
                    <path
                      d="M22.56 12.25c0-.78-.07-1.53-.2-2.25H12v4.26h5.92c-.26 1.37-1.04 2.53-2.21 3.31v2.77h3.57c2.08-1.92 3.28-4.74 3.28-8.09z"
                      fill="#4285F4"
                    />
                    <path
                      d="M12 23c2.97 0 5.46-.98 7.28-2.66l-3.57-2.77c-.98.66-2.23 1.06-3.71 1.06-2.86 0-5.29-1.93-6.16-4.53H2.18v2.84C3.99 20.53 7.7 23 12 23z"
                      fill="#34A853"
                    />
                    <path
                      d="M5.84 14.09c-.22-.66-.35-1.36-.35-2.09s.13-1.43.35-2.09V7.07H2.18C1.43 8.55 1 10.22 1 12s.43 3.45 1.18 4.93l2.85-2.22.81-.62z"
                      fill="#FBBC05"
                    />
                    <path
                      d="M12 5.38c1.62 0 3.06.56 4.21 1.64l3.15-3.15C17.45 2.09 14.97 1 12 1 7.7 1 3.99 3.47 2.18 7.07l3.66 2.84c.87-2.6 3.3-4.53 6.16-4.53z"
                      fill="#EA4335"
                    />
                  </svg>
                )}
                <span>{loading ? t('auth.connecting') : t('auth.loginWithGoogle')}</span>
              </button>

              {/* Other Login Options - Compact Row */}
              <div className="flex items-center justify-center space-x-4 pt-2">
                <span className={`text-sm ${isDark ? 'text-gray-400' : 'text-gray-600'}`}>{language === 'zh' ? '或使用' : 'Or sign in with'}</span>
              </div>

              <div className="flex items-center justify-center space-x-3">
                {/* GitHub Logo Button */}
                <button
                  disabled={true}
                  className="group relative p-3 rounded-lg bg-gray-800 hover:bg-gray-700 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  title={language === 'zh' ? 'GitHub 登录（即将推出）' : 'GitHub Login (Coming Soon)'}
                >
                  <Github className="w-5 h-5 text-white" />
                  <div
                    className={`absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs rounded ${
                      isDark ? 'bg-gray-900 text-gray-300' : 'bg-black text-white'
                    } opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none whitespace-nowrap`}
                  >
                    {language === 'zh' ? '即将推出' : 'Coming Soon'}
                  </div>
                </button>

                {/* Apple Logo Button - Placeholder */}
                <button
                  disabled={true}
                  className="group relative p-3 rounded-lg bg-black hover:bg-gray-800 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  title={language === 'zh' ? 'Apple 登录（即将推出）' : 'Apple Login (Coming Soon)'}
                >
                  <svg className="w-5 h-5 text-white" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M18.71 19.5c-.83 1.24-1.71 2.45-3.05 2.47-1.34.03-1.77-.79-3.29-.79-1.53 0-2 .77-3.27.82-1.31.05-2.3-1.32-3.14-2.53C4.25 17 2.94 12.45 4.7 9.39c.87-1.52 2.43-2.48 4.12-2.51 1.28-.02 2.5.87 3.29.87.78 0 2.26-1.07 3.81-.91.65.03 2.47.26 3.64 1.98-.09.06-2.17 1.28-2.15 3.81.03 3.02 2.65 4.03 2.68 4.04-.03.07-.42 1.44-1.38 2.83M13 3.5c.73-.83 1.94-1.46 2.94-1.5.13 1.17-.34 2.35-1.04 3.19-.69.85-1.83 1.51-2.95 1.42-.15-1.15.41-2.35 1.05-3.11z" />
                  </svg>
                  <div
                    className={`absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs rounded ${
                      isDark ? 'bg-gray-900 text-gray-300' : 'bg-black text-white'
                    } opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none whitespace-nowrap`}
                  >
                    {language === 'zh' ? '即将推出' : 'Coming Soon'}
                  </div>
                </button>

                {/* Microsoft Logo Button - Placeholder */}
                <button
                  disabled={true}
                  className="group relative p-3 rounded-lg bg-blue-500 hover:bg-blue-600 disabled:opacity-50 disabled:cursor-not-allowed transition-colors"
                  title={language === 'zh' ? 'Microsoft 登录（即将推出）' : 'Microsoft Login (Coming Soon)'}
                >
                  <svg className="w-5 h-5 text-white" viewBox="0 0 24 24" fill="currentColor">
                    <path d="M11.4 24H0V12.6h11.4V24zM24 24H12.6V12.6H24V24zM11.4 11.4H0V0h11.4v11.4zM24 11.4H12.6V0H24v11.4z" />
                  </svg>
                  <div
                    className={`absolute -top-8 left-1/2 transform -translate-x-1/2 px-2 py-1 text-xs rounded ${
                      isDark ? 'bg-gray-900 text-gray-300' : 'bg-black text-white'
                    } opacity-0 group-hover:opacity-100 transition-opacity pointer-events-none whitespace-nowrap`}
                  >
                    {language === 'zh' ? '即将推出' : 'Coming Soon'}
                  </div>
                </button>
              </div>
            </div>

            <div className={`text-center text-sm ${isDark ? 'text-gray-500' : 'text-gray-600'}`}>
              <p>{t('auth.termsAgreement')}</p>
            </div>
          </div>
        </div>

        <div className={`text-center text-xs ${isDark ? 'text-gray-600' : 'text-gray-500'}`}>
          <p>{t('auth.secureAuthGoogle')}</p>
        </div>
      </div>
    </div>
  );
};
