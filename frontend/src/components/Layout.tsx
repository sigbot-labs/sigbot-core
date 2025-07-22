import React, { useState, useCallback, useRef } from 'react';
import { BarChart3, Settings, Activity, LogOut, User, ChevronLeft, ChevronRight, Server } from 'lucide-react';
import { apiServiceFacade, ApiType } from '../services';
import { User as UserType } from '../types';
import { useTranslation } from 'react-i18next';
import { useTheme } from '../contexts/ThemeContext';
import { useLanguage } from '../hooks/useLanguage';
import { SignalTradingIcon } from './SignalTradingIcon';

interface LayoutProps {
  children: React.ReactNode;
  currentView: string;
  onViewChange: (view: string) => void;
  user: UserType;
  onLogout: () => void;
  sidebarCollapsed?: boolean;
  onSidebarCollapsedChange?: (collapsed: boolean) => void;
  onNavigateToWorkflowRun?: (workflowId: string, runId: string) => void;
}

export const Layout: React.FC<LayoutProps> = ({
  children,
  currentView,
  onViewChange,
  user,
  onLogout,
  sidebarCollapsed: externalSidebarCollapsed,
  onSidebarCollapsedChange,
  onNavigateToWorkflowRun,
}) => {
  const { t } = useTranslation();
  const { theme, setTheme, isDark } = useTheme();
  const { language, setLanguage } = useLanguage();
  const [showUserMenu, setShowUserMenu] = useState(false);
  const [internalSidebarCollapsed, setInternalSidebarCollapsed] = useState(false);
  const [showNotifications, setShowNotifications] = useState(false);
  const [apiMode, setApiMode] = useState<ApiType>(apiServiceFacade.getCurrentApiType());

  const handleApiModeChange = (mode: ApiType) => {
    setApiMode(mode);
    apiServiceFacade.switchService(mode);
  };

  // Use API service to fetch notification data
  const [notifications, setNotifications] = useState<
    Array<{
      id: string;
      title: string;
      message: string;
      level: string;
      type: string;
      read: boolean;
      timestamp: Date;
      workflowId?: string;
      workflowRunId?: string;
    }>
  >([]);
  const [notificationsLoading, setNotificationsLoading] = useState(false);
  const loadingRef = useRef(false);

  // Fetch notification data
  const fetchNotifications = useCallback(async () => {
    if (loadingRef.current) return;
    console.debug('🔔 Fetching notifications...');
    loadingRef.current = true;
    setNotificationsLoading(true);
    try {
      const data = await apiServiceFacade.getService().getNotifications();
      console.debug('✅ Notifications loaded:', data.length, 'notifications');
      setNotifications(data);
    } catch (error) {
      console.error('❌ Failed to fetch notifications:', error);
    } finally {
      loadingRef.current = false;
      setNotificationsLoading(false);
    }
  }, []);

  // Mark notification as read
  const handleMarkAsRead = async (notificationId: string) => {
    try {
      await apiServiceFacade.getService().markNotificationAsRead(notificationId);
      setNotifications(prev => prev.map(notif => (notif.id === notificationId ? { ...notif, read: true } : notif)));
    } catch (error) {
      console.error('Failed to mark notification as read:', error);
    }
  };

  // Mark all notifications as read
  const handleMarkAllAsRead = async () => {
    try {
      await apiServiceFacade.getService().markAllNotificationsAsRead();
      setNotifications(prev => prev.map(notif => ({ ...notif, read: true })));
    } catch (error) {
      console.error('Failed to mark all notifications as read:', error);
    }
  };

  // Handle notification click
  const handleNotificationClick = (notification: {
    id: string;
    title: string;
    message: string;
    level: string;
    type: string;
    read: boolean;
    timestamp: Date;
    workflowId?: string;
    workflowRunId?: string;
  }) => {
    // If it's a transaction type notification with workflowRunId, navigate to editor
    if (notification.type === 'Transaction' && notification.workflowRunId && onNavigateToWorkflowRun) {
      onNavigateToWorkflowRun(notification.workflowId!, notification.workflowRunId);
      setShowNotifications(false);
      // Mark as read
      if (!notification.read) {
        handleMarkAsRead(notification.id);
      }
    }
  };

  // Auto-fetch notifications on component mount and when notifications are shown
  React.useEffect(() => {
    console.debug('📱 Layout mounted, auto-loading notifications...');
    // Auto-load notifications when component mounts (user is authenticated)
    fetchNotifications();
  }, [fetchNotifications]); // Run once on mount

  // Fetch data when notifications dropdown is shown (for refreshing)
  React.useEffect(() => {
    if (showNotifications) {
      fetchNotifications();
    }
  }, [showNotifications, fetchNotifications]);

  const unreadCount = notifications.filter(n => !n.read).length;

  // Use external state or internal state
  const sidebarCollapsed = externalSidebarCollapsed !== undefined ? externalSidebarCollapsed : internalSidebarCollapsed;
  const setSidebarCollapsed = (collapsed: boolean) => {
    if (onSidebarCollapsedChange) {
      onSidebarCollapsedChange(collapsed);
    } else {
      setInternalSidebarCollapsed(collapsed);
    }
  };

  const navItems = [
    { id: 'DASHBOARD', label: t('navigation.dashboard'), icon: Activity },
    { id: 'WORKFLOWS', label: t('navigation.workflows'), icon: BarChart3 },
  ];

  // Close menus when clicking outside
  const handleClickOutside = (event: MouseEvent) => {
    const target = event.target as Element;
    if (!target.closest('.menu-container')) {
      setShowUserMenu(false);
      setShowNotifications(false);
    }
  };

  // Add click outside listener
  React.useEffect(() => {
    document.addEventListener('mousedown', handleClickOutside);
    return () => {
      document.removeEventListener('mousedown', handleClickOutside);
    };
  }, []);

  const getNotificationIcon = (level: string) => {
    switch (level) {
      case 'success':
        return '✅';
      case 'warning':
        return '⚠️';
      case 'error':
        return '❌';
      case 'info':
        return 'ℹ️';
      default:
        return '📢';
    }
  };

  const formatTimeAgo = (timestamp: Date) => {
    const now = new Date();
    const diff = now.getTime() - timestamp.getTime();
    const minutes = Math.floor(diff / 60000);
    const hours = Math.floor(diff / 3600000);
    const days = Math.floor(diff / 86400000);

    let timeAgo = '';
    if (days > 0) timeAgo = `${days}d ago`;
    else if (hours > 0) timeAgo = `${hours}h ago`;
    else if (minutes > 0) timeAgo = `${minutes}m ago`;
    else timeAgo = 'now';

    // Compact time format: 3d ago | 25/03/21 01:11:44 UTC | 25/03/21 09:11:44
    const utcTime = timestamp
      .toISOString()
      .replace('T', ' ')
      .replace(/\.\d{3}Z/, '');
    const utcFormatted = utcTime.substring(2).replace('-', '/').replace('-', '/'); // Remove first 2 digits of year

    const localTime = timestamp
      .toLocaleString('en-GB', {
        year: '2-digit',
        month: '2-digit',
        day: '2-digit',
        hour: '2-digit',
        minute: '2-digit',
        second: '2-digit',
        hour12: false,
      })
      .replace(/\//g, '/');

    return `${timeAgo} | ${utcFormatted} UTC | ${localTime}`;
  };

  return (
    <div
      className={`h-screen ${isDark ? 'bg-gray-900 text-white' : 'bg-gray-50 text-gray-900'} flex`}
      data-sidebar-collapsed={sidebarCollapsed}
    >
      {/* Sidebar */}
      <div
        className={`${sidebarCollapsed ? 'w-16' : 'w-64'} ${
          isDark ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
        } border-r transition-all duration-300 relative`}
      >
        {/* Collapse Toggle Button */}
        <button
          onClick={() => setSidebarCollapsed(!sidebarCollapsed)}
          className={`absolute -right-3 top-6 z-10 ${
            isDark ? 'bg-gray-700 hover:bg-gray-600 border-gray-600' : 'bg-gray-200 hover:bg-gray-300 border-gray-300'
          } rounded-full p-1 border transition-colors`}
          title={sidebarCollapsed ? t('common.expand') : t('common.collapse')}
        >
          {sidebarCollapsed ? (
            <ChevronRight className={`w-4 h-4 ${isDark ? 'text-gray-300' : 'text-gray-600'}`} />
          ) : (
            <ChevronLeft className={`w-4 h-4 ${isDark ? 'text-gray-300' : 'text-gray-600'}`} />
          )}
        </button>

        <div className="p-6">
          {!sidebarCollapsed ? (
            <div className="flex items-center space-x-3">
              <SignalTradingIcon size="large" />
              <div>
                <h1 className="text-xl font-bold text-blue-400">Signal Trading</h1>
                <p className={`text-sm ${isDark ? 'text-gray-400' : 'text-gray-600'} mt-1`}>Focus on AI Trading</p>
              </div>
            </div>
          ) : (
            <div className="flex flex-col justify-center items-center min-h-[64px] -mt-5 hover:scale-100 transition-transform cursor-pointer select-none" title="Signal Trading">
              <SignalTradingIcon size="medium" className="w-12 h-12 hover:scale-110 transition-transform ml-[-8px]" />
            </div>
          )}

          {/* API Mode display - moved to top */}
          {!sidebarCollapsed && (
            <div className="mt-4 pt-4 border-t border-gray-600">
              <div className={`text-xs ${isDark ? 'text-gray-400' : 'text-gray-600'} flex items-center justify-between`}>
                <span>API Mode:</span>
                <span
                  className={`px-2 py-1 rounded text-xs font-medium ${
                    apiServiceFacade.getCurrentApiType() === 'MOCK'
                      ? 'bg-yellow-100 text-yellow-700 dark:bg-yellow-900 dark:text-yellow-300'
                      : 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300'
                  }`}
                >
                  {apiServiceFacade.getCurrentApiType()}
                </span>
              </div>
            </div>
          )}
        </div>

        <nav className="mt-8">
          {navItems.map(item => {
            const Icon = item.icon;
            return (
              <button
                key={item.id}
                onClick={() => onViewChange(item.id)}
                className={`w-full flex items-center ${
                  sidebarCollapsed ? 'justify-center px-3' : 'px-6'
                } py-3 text-left transition-colors ${
                  currentView === item.id
                    ? 'bg-blue-600 text-white border-r-2 border-blue-400'
                    : `${isDark ? 'text-gray-300 hover:bg-gray-700' : 'text-gray-700 hover:bg-gray-100'}`
                }`}
                title={sidebarCollapsed ? item.label : undefined}
              >
                <Icon className="w-5 h-5 mr-3" />
                {!sidebarCollapsed && item.label}
              </button>
            );
          })}
        </nav>
      </div>

      {/* Main Content */}
      <div className="flex-1 overflow-visible flex flex-col">
        {/* Top Bar */}
        <div
          className={`${
            isDark ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
          } border-b px-6 py-3 flex items-center justify-between`}
        >
          {/* Left side - Empty or Logo */}
          <div className="flex items-center">{/* 可以在这里添加面包屑导航或其他左侧内容 */}</div>

          {/* Right side - Strategies Stats, Notifications & User Menu */}
          <div className="flex items-center space-x-4">
            {/* Strategy Statistics - Compact */}
            <div className={`flex items-center space-x-3 ${isDark ? 'bg-gray-700' : 'bg-gray-100'} rounded-lg px-3 py-1.5`}>
              <div className="flex items-center space-x-2">
                <span className={`${isDark ? 'text-gray-400' : 'text-gray-600'} text-xs font-medium`}>Strategies:</span>
              </div>
              <div className="flex items-center space-x-2">
                <div className="w-2 h-2 bg-green-400 rounded-full"></div>
                <span className="text-green-400 font-medium text-sm">5</span>
                <span className={`${isDark ? 'text-gray-400' : 'text-gray-600'} text-xs`}>Active</span>
              </div>
              <div className="w-px h-4 bg-gray-400 opacity-50"></div>
              <div className="flex items-center space-x-2">
                <div className="w-2 h-2 bg-yellow-400 rounded-full"></div>
                <span className="text-yellow-400 font-medium text-sm">2</span>
                <span className={`${isDark ? 'text-gray-400' : 'text-gray-600'} text-xs`}>Paused</span>
              </div>
              <div className="w-px h-4 bg-gray-400 opacity-50"></div>
              <div className="flex items-center space-x-2">
                <span className={`${isDark ? 'text-gray-300' : 'text-gray-700'} font-medium text-sm`}>7</span>
                <span className={`${isDark ? 'text-gray-400' : 'text-gray-600'} text-xs`}>Total</span>
              </div>
            </div>

            {/* Notifications Bell - Compact */}
            <div className="relative menu-container">
              <button
                className={`p-2 rounded-lg ${isDark ? 'hover:bg-gray-700' : 'hover:bg-gray-100'} transition-colors`}
                onClick={() => {
                  setShowNotifications(!showNotifications);
                  setShowUserMenu(false); // Close user menu
                }}
                title={t('navigation.notifications')}
              >
                <div className="relative">
                  <svg
                    className={`w-5 h-5 ${isDark ? 'text-gray-400' : 'text-gray-600'}`}
                    fill="none"
                    stroke="currentColor"
                    viewBox="0 0 24 24"
                  >
                    <path
                      strokeLinecap="round"
                      strokeLinejoin="round"
                      strokeWidth={2}
                      d="M15 17h5l-1.405-1.405A2.032 2.032 0 0118 14.158V11a6.002 6.002 0 00-4-5.659V5a2 2 0 10-4 0v.341C7.67 6.165 6 8.388 6 11v3.159c0 .538-.214 1.055-.595 1.436L4 17h5m6 0v1a3 3 0 11-6 0v-1m6 0H9"
                    />
                  </svg>
                  {/* Notification Badge */}
                  {unreadCount > 0 && (
                    <div
                      className={`absolute -top-1 -right-1 w-5 h-5 bg-red-500 rounded-full flex items-center justify-center text-xs font-medium text-white`}
                    >
                      {unreadCount > 9 ? '9+' : unreadCount}
                    </div>
                  )}
                </div>
              </button>

              {/* Notifications Dropdown */}
              {showNotifications && (
                <div
                  className={`absolute right-0 mt-2 w-96 ${
                    isDark ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
                  } rounded-lg shadow-xl border z-50 flex flex-col`}
                  style={{ maxHeight: '24rem' }}
                >
                  {/* Header */}
                  <div className={`px-4 py-3 border-b ${isDark ? 'border-gray-700' : 'border-gray-200'} flex items-center justify-between`}>
                    <div className={`text-sm font-medium ${isDark ? 'text-white' : 'text-gray-900'}`}>{t('navigation.notifications')}</div>
                    {unreadCount > 0 && (
                      <div
                        className={`text-xs px-2 py-1 rounded-full ${isDark ? 'bg-blue-600 text-blue-100' : 'bg-blue-100 text-blue-700'}`}
                      >
                        {unreadCount} {t('notifications.unreadCount')}
                      </div>
                    )}
                  </div>

                  {/* Notification List */}
                  <div className="flex-1 overflow-y-auto" style={{ maxHeight: '16rem' }}>
                    {notificationsLoading ? (
                      <div className={`px-4 py-8 text-center ${isDark ? 'text-gray-400' : 'text-gray-500'}`}>
                        <div className="w-6 h-6 border-2 border-blue-500 border-t-transparent rounded-full animate-spin mx-auto mb-2"></div>
                        <p className="text-sm">{t('notifications.loading')}</p>
                      </div>
                    ) : notifications.length > 0 ? (
                      notifications.map(notification => (
                        <div
                          key={notification.id}
                          onClick={() => handleNotificationClick(notification)}
                          className={`px-4 py-3 border-b ${
                            isDark ? 'border-gray-700 hover:bg-gray-700' : 'border-gray-100 hover:bg-gray-50'
                          } transition-colors ${!notification.read ? (isDark ? 'bg-blue-900 bg-opacity-20' : 'bg-blue-50') : ''} ${
                            notification.type === 'Transaction' && notification.workflowRunId ? 'cursor-pointer' : 'cursor-default'
                          }`}
                        >
                          <div className="flex items-start space-x-3">
                            <div className="flex-shrink-0 text-lg">{getNotificationIcon(notification.level)}</div>
                            <div className="flex-1 min-w-0">
                              <div className="flex items-center justify-between">
                                <div className="flex items-center space-x-2">
                                  <p
                                    className={`text-sm font-medium ${isDark ? 'text-white' : 'text-gray-900'} ${
                                      !notification.read ? 'font-semibold' : ''
                                    }`}
                                  >
                                    {notification.title}
                                  </p>
                                  {/* Show notification type */}
                                  <span
                                    className={`px-2 py-0.5 text-xs rounded-full ${
                                      notification.type === 'Transaction'
                                        ? 'bg-green-100 text-green-700 dark:bg-green-900 dark:text-green-300'
                                        : 'bg-gray-100 text-gray-700 dark:bg-gray-700 dark:text-gray-300'
                                    }`}
                                  >
                                    {notification.type}
                                  </span>
                                </div>
                                {!notification.read && <div className="w-2 h-2 bg-blue-500 rounded-full"></div>}
                              </div>
                              <p className={`text-sm ${isDark ? 'text-gray-400' : 'text-gray-600'} mt-1`}>{notification.message}</p>
                              {/* jump to workflow run */}
                              {notification.type === 'Transaction' && notification.workflowRunId && (
                                <p className={`text-xs ${isDark ? 'text-blue-400' : 'text-blue-600'} mt-1 italic`}>
                                  {t('dashboard.viewDetails')} →
                                </p>
                              )}
                              <p className={`text-xs ${isDark ? 'text-gray-500' : 'text-gray-500'} mt-2`}>
                                {formatTimeAgo(notification.timestamp)}
                              </p>
                            </div>
                          </div>
                        </div>
                      ))
                    ) : (
                      <div className={`px-4 py-8 text-center ${isDark ? 'text-gray-400' : 'text-gray-500'}`}>
                        <div className="text-3xl mb-2">🔔</div>
                        <p className="text-sm">{t('notifications.noNotifications')}</p>
                      </div>
                    )}
                  </div>

                  {/* Footer */}
                  <div
                    className={`px-4 py-3 border-t ${
                      isDark ? 'border-gray-700' : 'border-gray-200'
                    } flex items-center justify-between flex-shrink-0`}
                  >
                    <button
                      onClick={handleMarkAllAsRead}
                      disabled={unreadCount === 0 || notificationsLoading}
                      className={`text-sm ${
                        unreadCount > 0 && !notificationsLoading
                          ? isDark
                            ? 'text-blue-400 hover:text-blue-300'
                            : 'text-blue-600 hover:text-blue-500'
                          : 'text-gray-400 cursor-not-allowed'
                      } transition-colors`}
                    >
                      {t('notifications.markAllAsRead')}
                    </button>
                    <button
                      onClick={() => setShowNotifications(false)}
                      className={`text-sm ${
                        isDark ? 'text-gray-400 hover:text-gray-300' : 'text-gray-600 hover:text-gray-500'
                      } transition-colors`}
                    >
                      {t('common.close')}
                    </button>
                  </div>
                </div>
              )}
            </div>

            {/* User Info */}
            <div className="relative menu-container">
              <button
                onClick={() => setShowUserMenu(!showUserMenu)}
                className={`flex items-center space-x-3 px-3 py-2 rounded-lg ${
                  isDark ? 'hover:bg-gray-700' : 'hover:bg-gray-100'
                } transition-colors`}
              >
                {user.avatar ? (
                  <img src={user.avatar} alt={user.name} className="w-8 h-8 rounded-full" />
                ) : (
                  <User className={`w-8 h-8 ${isDark ? 'text-gray-400' : 'text-gray-600'}`} />
                )}
                <div className="text-left">
                  <p className={`text-sm font-medium ${isDark ? 'text-white' : 'text-gray-900'}`}>{user.name}</p>
                  <p className={`text-xs ${isDark ? 'text-gray-400' : 'text-gray-600'}`}>{user.email}</p>
                </div>
              </button>

              {showUserMenu && (
                <div
                  className={`absolute right-0 mt-2 w-64 ${
                    isDark ? 'bg-gray-800 border-gray-700' : 'bg-white border-gray-200'
                  } rounded-lg shadow-lg border py-2 z-50`}
                >
                  {/* User Info Section */}
                  <div className={`px-4 py-3 border-b ${isDark ? 'border-gray-700' : 'border-gray-200'}`}>
                    <div className={`text-sm font-medium ${isDark ? 'text-white' : 'text-gray-900'}`}>{user.name}</div>
                    <div className={`text-sm ${isDark ? 'text-gray-400' : 'text-gray-600'}`}>{user.email}</div>
                  </div>

                  {/* Menu Items */}
                  <div className="py-1">
                    {/* Settings Group */}
                    <button
                      onClick={() => {
                        onViewChange('settings');
                        setShowUserMenu(false);
                      }}
                      className={`w-full flex items-center space-x-3 px-4 py-2 text-left ${
                        isDark ? 'text-gray-300 hover:bg-gray-700' : 'text-gray-700 hover:bg-gray-100'
                      } transition-colors`}
                    >
                      <Settings className="w-4 h-4" />
                      <div>
                        <div className="text-sm font-medium">{t('navigation.settings')}</div>
                        <div className={`text-xs ${isDark ? 'text-gray-500' : 'text-gray-500'}`}>Trading & General Settings</div>
                      </div>
                    </button>

                    {/* Theme & Language & Service Mode */}
                    <div className={`px-4 py-2 border-t ${isDark ? 'border-gray-700' : 'border-gray-200'} mt-2`}>
                      <div className={`text-xs font-medium ${isDark ? 'text-gray-400' : 'text-gray-500'} mb-2`}>PREFERENCES</div>

                      {/* Quick Theme Toggle */}
                      <div className="flex items-center justify-between mb-2">
                        <span className={`text-sm ${isDark ? 'text-gray-300' : 'text-gray-700'}`}>{t('settings.theme')}</span>
                        <div className="flex items-center space-x-1">
                          {(['light', 'dark'] as const).map(themeOption => (
                            <button
                              key={themeOption}
                              onClick={() => setTheme(themeOption)}
                              className={`px-2 py-1 text-xs rounded transition-colors ${
                                theme === themeOption
                                  ? 'bg-blue-600 text-white'
                                  : `${
                                      isDark ? 'bg-gray-700 text-gray-300 hover:bg-gray-600' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                                    }`
                              }`}
                            >
                              {themeOption === 'light' ? '☀️' : '🌙'}
                            </button>
                          ))}
                        </div>
                      </div>

                      {/* Quick Language Toggle */}
                      <div className="flex items-center justify-between mb-2">
                        <span className={`text-sm ${isDark ? 'text-gray-300' : 'text-gray-700'}`}>{t('settings.language')}</span>
                        <div className="flex items-center space-x-1">
                          {(['en', 'zh'] as const).map(langOption => (
                            <button
                              key={langOption}
                              onClick={() => setLanguage(langOption)}
                              className={`px-2 py-1 text-xs rounded transition-colors ${
                                language === langOption
                                  ? 'bg-blue-600 text-white'
                                  : `${
                                      isDark ? 'bg-gray-700 text-gray-300 hover:bg-gray-600' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                                    }`
                              }`}
                            >
                              {langOption === 'en' ? 'EN' : '中'}
                            </button>
                          ))}
                        </div>
                      </div>

                      {/* Quick API Mode Toggle */}
                      <div className="flex items-center justify-between">
                        <span className={`text-sm ${isDark ? 'text-gray-300' : 'text-gray-700'}`}>{t('settings.apiMode')}</span>
                        <div className="flex items-center space-x-1">
                          {(['MOCK', 'API'] as const).map(modeOption => (
                            <button
                              key={modeOption}
                              onClick={() => handleApiModeChange(modeOption)}
                              className={`px-2 py-1 text-xs rounded transition-colors ${
                                apiMode === modeOption
                                  ? 'bg-blue-600 text-white'
                                  : `${
                                      isDark ? 'bg-gray-700 text-gray-300 hover:bg-gray-600' : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                                    }`
                              }`}
                              title={t(`settings.${modeOption === 'MOCK' ? 'mockMode' : 'httpMode'}`)}
                            >
                              {modeOption}
                            </button>
                          ))}
                        </div>
                      </div>
                    </div>

                    {/* Logout */}
                    <div className={`border-t ${isDark ? 'border-gray-700' : 'border-gray-200'} mt-2 pt-2`}>
                      <button
                        onClick={onLogout}
                        className={`w-full flex items-center space-x-2 px-4 py-2 text-left ${
                          isDark ? 'text-gray-300 hover:bg-gray-700' : 'text-gray-700 hover:bg-gray-100'
                        } transition-colors`}
                      >
                        <LogOut className="w-4 h-4" />
                        <span>{t('auth.logout')}</span>
                      </button>
                    </div>
                  </div>
                </div>
              )}
            </div>
          </div>
        </div>
        {/* Content */}
        <div className="flex-1 overflow-y-auto">{children}</div>
      </div>
    </div>
  );
};
