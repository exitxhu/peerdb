import { NextRequest, NextResponse } from 'next/server';
import { withAuth } from 'next-auth/middleware';
import { CloudToken } from '@/app/api/auth/[...nextauth]/route';


const authMiddleware = withAuth(
  {
    callbacks: {
      authorized: async ({ token, req }) => {
        const sessionToken = req.cookies.get('next-auth.session-token');
        if (sessionToken == null) {
          return false;
        }
        const orgInfo = (token as CloudToken).profile.org_id_to_org_member_info[process.env.PEERDB_CLOUD_ORG_ID!];
        if (orgInfo == null) {
          return false;
        }
        return orgInfo.inherited_user_roles_plus_current_role.includes('Member');

      },
    },
  },
);


export default async function middleware(req: NextRequest, resp: NextResponse) {
  return (authMiddleware as any)(req);
}

export const config = {
  matcher: [
    // Match everything other than static assets
    '/((?!_next/static/|images/|favicon.ico$).*)',
  ],
};
