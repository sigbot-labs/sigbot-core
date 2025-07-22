import CryptoJS from 'crypto-js';
import { User } from '../types';

// Generate AES key based on current domain hash
const generateAESKey = (): string => {
  const domain = window.location.hostname || 'localhost';
  return CryptoJS.SHA256(domain + '@signal-trading-security').toString();
};

// User session interface with permissions
export interface UserSession {
  user: User;
  token: string;
  permissions: string[];
  loginTime: number;
  expiresAt: number;
}

export class AuthStorageService {
  private static readonly STORAGE_KEY = 'st_user_session';
  private static readonly SESSION_DURATION = 7 * 24 * 60 * 60 * 1000; // 7 days
  private aesKey: string;

  constructor() {
    this.aesKey = generateAESKey();
  }

  // Encrypt data using AES
  private encrypt(data: string): string {
    return CryptoJS.AES.encrypt(data, this.aesKey).toString();
  }

  // Decrypt data using AES
  private decrypt(encryptedData: string): string {
    const bytes = CryptoJS.AES.decrypt(encryptedData, this.aesKey);
    return bytes.toString(CryptoJS.enc.Utf8);
  }

  // Store encrypted user session
  storeUserSession(user: User, token: string, permissions: string[] = []): void {
    const session: UserSession = {
      user,
      token,
      permissions,
      loginTime: Date.now(),
      expiresAt: Date.now() + AuthStorageService.SESSION_DURATION
    };

    const sessionJson = JSON.stringify(session);
    const encryptedSession = this.encrypt(sessionJson);
    localStorage.setItem(AuthStorageService.STORAGE_KEY, encryptedSession);
    
    // Also store token separately for backward compatibility
    localStorage.setItem('auth_token', token);
  }

  // Retrieve and decrypt user session
  getUserSession(): UserSession | null {
    try {
      const encryptedSession = localStorage.getItem(AuthStorageService.STORAGE_KEY);
      if (!encryptedSession) {
        return null;
      }

      const sessionJson = this.decrypt(encryptedSession);
      const session: UserSession = JSON.parse(sessionJson);

      // Check if session is expired
      if (Date.now() > session.expiresAt) {
        this.clearUserSession();
        return null;
      }

      return session;
    } catch (error) {
      console.error('Failed to decrypt user session:', error);
      this.clearUserSession();
      return null;
    }
  }

  // Get current user info
  getCurrentUser(): User | null {
    const session = this.getUserSession();
    return session ? session.user : null;
  }

  // Get user token
  getToken(): string | null {
    const session = this.getUserSession();
    return session ? session.token : null;
  }

  // Get user permissions
  getUserPermissions(): string[] {
    const session = this.getUserSession();
    return session ? session.permissions : [];
  }

  // Check if user has specific permission
  hasPermission(permission: string): boolean {
    const permissions = this.getUserPermissions();
    return permissions.includes(permission);
  }

  // Update user permissions
  updatePermissions(permissions: string[]): void {
    const session = this.getUserSession();
    if (session) {
      session.permissions = permissions;
      const sessionJson = JSON.stringify(session);
      const encryptedSession = this.encrypt(sessionJson);
      localStorage.setItem(AuthStorageService.STORAGE_KEY, encryptedSession);
    }
  }

  // Clear user session
  clearUserSession(): void {
    localStorage.removeItem(AuthStorageService.STORAGE_KEY);
    localStorage.removeItem('auth_token');
  }

  // Check if session is valid
  isSessionValid(): boolean {
    const session = this.getUserSession();
    return session !== null;
  }

  // Extend session expiration
  extendSession(): void {
    const session = this.getUserSession();
    if (session) {
      session.expiresAt = Date.now() + AuthStorageService.SESSION_DURATION;
      const sessionJson = JSON.stringify(session);
      const encryptedSession = this.encrypt(sessionJson);
      localStorage.setItem(AuthStorageService.STORAGE_KEY, encryptedSession);
    }
  }
}

// Singleton instance
export const authStorage = new AuthStorageService(); 