/**
 * Subscription authorization seam.
 *
 *   authenticate → resolve resource → authorize → subscribe
 *
 * A client must only receive events for resources it may access. "Authenticated" is not
 * "may subscribe to anything" — that decision routes through {@link SubscriptionAuthorizer}.
 */
import type { Logger } from '@forge/logging';
import type { Principal } from './auth.js';
import type { SubscriptionTarget } from './protocol.js';

export interface SubscriptionAuthorizer {
  /**
   * @returns `true` if `principal` may subscribe to `target`. May be async (a real impl
   *          will resolve the resource + its project/owner and check membership).
   */
  authorize(principal: Principal, target: SubscriptionTarget): boolean | Promise<boolean>;
}

/**
 * ⚠️ PLACEHOLDER. Forge has no resource-ownership model yet (no users, no projects, no
 * pipeline/run/job ownership rows — see `docs/architecture/*`). Until that exists there is
 * nothing to check `target` against, so any authenticated principal may subscribe to any
 * well-formed target. This is a deliberate, documented gap: swap this implementation for a
 * resource-aware one the moment the API grows an ownership model. It exists as a seam so
 * that swap touches exactly one wiring point.
 */
export class AllowAuthenticatedAuthorizer implements SubscriptionAuthorizer {
  constructor(private readonly logger?: Logger) {}

  public authorize(principal: Principal, target: SubscriptionTarget): boolean {
    this.logger?.debug('websocket.authorization_placeholder_allow', {
      principal_id: principal.id,
      target_kind: target.kind,
    });
    return true;
  }
}
