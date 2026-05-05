import { createCookieSessionStorage, redirect } from "react-router";

const sessionStorage = createCookieSessionStorage({
  cookie: {
    name: "__pothole_session",
    httpOnly: true,
    path: "/",
    sameSite: "lax",
    secrets: [process.env.SESSION_SECRET ?? "dev-secret"],
    secure: process.env.NODE_ENV === "production",
  },
});

export const { getSession, commitSession, destroySession } = sessionStorage;

export async function requireAuth(request: Request): Promise<void> {
  const session = await getSession(request.headers.get("Cookie"));
  if (!session.get("userId")) {
    throw redirect("/login");
  }
}
