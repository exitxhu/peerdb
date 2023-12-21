import NextAuth, { AuthOptions, Session } from 'next-auth';
import { OAuthConfig, OAuthUserConfig } from 'next-auth/providers/oauth';
import prisma from '@/app/utils/prisma';
import { PrismaAdapter } from '@next-auth/prisma-adapter';
import { JWT } from 'next-auth/jwt';


// This is specific to PeerDB
export interface CloudAuthProfile extends Record<string, any> {
  user_id: string

  email: string
  email_confirmed: boolean,

  has_password: boolean,

  username?: string
  first_name?: string,
  last_name?: string,
  picture_url?: string,

  locked: boolean,
  enabled: boolean,
  mfa_enabled: boolean,

  created_at: number,
  last_active_at: number,

  legacy_user_id?: string
  properties: { [key: string]: unknown }


  org_id_to_org_info: OrgIdInformation
}

type OrgIdInformation = {
  [key: string]: {
    org_id: string,
    inherited_user_roles_plus_current_role: string[],
    org_metadata: Record<string, any>,
    org_name: string,
    url_safe_org_name: string
    user_permissions: string[],
    user_role: string,
  }
}


export function CloudAuth<P extends CloudAuthProfile>(
  cloudAuthBaseUrl: string,
  options: OAuthUserConfig<P>,
): OAuthConfig<P> {
  return {
    id: 'oauth',
    name: 'PeerDB Cloud',
    type: 'oauth',
    wellKnown: `${cloudAuthBaseUrl}/.well-known/openid-configuration`,
    clientId: options.clientId,
    clientSecret: options.clientSecret,
    profile(profile: P) {
      return {
        id: profile.user_id,
        name: `${profile.first_name} ${profile.last_name}`,
        email: profile.email,
        // image: profile.picture_url,
      };
    },
    options,
  };
}


export type CloudToken = JWT & {
  profile: CloudAuthProfile
}

export interface CloudSession extends Session {
  profile: CloudAuthProfile
}


const propelAuthProvider = CloudAuth(
  process.env.OIDC_PROVIDER_BASE_URL!,
  {
    clientId: process.env.OAUTH_CLIENT_ID!,
    clientSecret: process.env.OAUTH_CLIENT_SECRET!,
  },
);

export const authOptions: AuthOptions = {
  providers: [
    propelAuthProvider,
  ],
  debug: true,
  session: {
    strategy: 'jwt',
    maxAge: 60 * 60, // 1h
  },
  adapter: PrismaAdapter(prisma),
  secret: process.env.NEXTAUTH_SECRET,
  theme: {
    colorScheme: 'light',
    logo: '/images/peerdb-combinedMark.svg',

  },
  callbacks: {
    async session({ token, session, user }) {
      const newSession = session as CloudSession
      newSession.profile = (token as CloudToken).profile;
      return newSession;
    },
    async jwt({ token, user, account, profile }) {
      token.profile = profile;
      return token;
    },
  },
};


const handler = NextAuth(authOptions);

export { handler as GET, handler as POST };
