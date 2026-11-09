import { useCallback, useEffect, useState } from "react";

export class ApiError extends Error {
  constructor(
    public status: number,
    message: string,
  ) {
    super(message);
  }
}

export class Api {
  static async request<T>(path: string, init: RequestInit = {}): Promise<T> {
    const response = await fetch(path, {
      credentials: "same-origin",
      ...init,
      headers: {
        Accept: "application/json",
        ...(init.body ? { "Content-Type": "application/json" } : {}),
        ...init.headers,
      },
    });
    if (!response.ok)
      throw new ApiError(
        response.status,
        response.status === 401
          ? "请通过 Authguard 登录后连接工作台"
          : response.status === 403
            ? "当前身份没有此资源的访问权限"
            : response.status === 404
              ? "此服务尚未连接"
              : `服务暂不可用（${response.status}）`,
      );
    return response.status === 204
      ? (undefined as T)
      : (response.json() as Promise<T>);
  }
  static save<T>(path: string, body: unknown): Promise<T> {
    return Api.request(path, { method: "POST", body: JSON.stringify(body) });
  }
  static entity<T>(collection: string, env: string, fields: Record<string, unknown>, id?: string): Promise<T> {
    const path = `/api/v1/${collection}${id ? `/${id}` : ""}?env=${encodeURIComponent(env)}`;
    return Api.request(path, {method: id ? "PUT" : "POST", body: JSON.stringify(fields)});
  }
}

export function useRemote<T>(path: string, initial: T) {
  const [data, setData] = useState(initial);
  const [error, setError] = useState<string | null>(null);
  const [loading, setLoading] = useState(true);
  const [revision, setRevision] = useState(0);
  const refresh = useCallback(() => setRevision((value) => value + 1), []);
  useEffect(() => {
    const controller = new AbortController();
    setLoading(true);
    setData(initial);
    Api.request<T>(path, { signal: controller.signal })
      .then((value) => {
        if (!controller.signal.aborted) {
          setData(value);
          setError(null);
        }
      })
      .catch((error) => {
        if (!controller.signal.aborted)
          setError(error instanceof Error ? error.message : "网络连接失败");
      })
      .finally(() => {
        if (!controller.signal.aborted) setLoading(false);
      });
    return () => controller.abort();
  }, [path, revision]);
  return { data, error, loading, refresh };
}
