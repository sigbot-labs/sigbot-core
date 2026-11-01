import { createContext, useContext } from "react";
import type { DeploymentEnvironment } from "./types";
export interface Workspace {
  env: DeploymentEnvironment;
  setEnv: (env: DeploymentEnvironment) => void;
}
export const WorkspaceContext = createContext<Workspace>({
  env: "dev",
  setEnv: () => {},
});
export function useScope() {
  const workspace = useContext(WorkspaceContext);
  return {
    ...workspace,
    query: `env=${workspace.env}`,
  };
}
