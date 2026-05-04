import type { Route } from "./+types/_app";
import { AppShell } from "~/components/AppShell";
import { requireAuth } from "~/lib/session.server";

export async function loader({ request }: Route.LoaderArgs) {
  await requireAuth(request);
  return null;
}

export default function AppLayout() {
  return <AppShell />;
}
