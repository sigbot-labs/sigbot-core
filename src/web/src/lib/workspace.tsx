import { createContext, useContext } from "react";
import type { Environment } from "./types";
export interface Workspace {
  env: Environment;
  spaceId: number;
  setEnv: (env: Environment) => void;
  setSpaceId: (space: number) => void;
}
export const WorkspaceContext = createContext<Workspace>({
  env: "paper",
  spaceId: 1,
  setEnv: () => {},
  setSpaceId: () => {},
});
export function useScope() {
  const workspace = useContext(WorkspaceContext);
  return {
    ...workspace,
    query: `env=${workspace.env}&space_id=${workspace.spaceId}`,
  };
}
