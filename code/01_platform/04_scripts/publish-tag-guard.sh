#!/usr/bin/env bash
# publish-tag-guard.sh — a tag may only name the commit that is actually being published.
#
# Why this exists: publish-images.yml always checks out the DEFAULT BRANCH and commits the digest
# fragment back to it — that is what keeps a publish durable, since a digest living only in a run
# log is not. A `push: tags` trigger therefore publishes the default branch tip whatever the tag
# points at, so a tag sitting on an older commit would label digests with a version they do not
# describe. This guard refuses that instead of publishing something mislabelled.
#
# The tag is resolved inside the checkout rather than trusting the event payload: `^{commit}` peels
# an annotated tag to the commit it names, and a shallow, tag-less clone (what actions/checkout
# produces) is allowed to fetch the one tag it is asked about. Nothing here depends on GitHub's
# choice of `github.sha` for a tag push.
#
# Usage: publish-tag-guard.sh <tag-name>
#   Run from the root of a checkout of the default branch.
# Exit: 0 the tag names the checked-out commit
#       1 it does not name it, or the tag cannot be resolved
#       2 usage error
set -euo pipefail

usage() {
  echo "usage: publish-tag-guard.sh <tag-name>   (run from a checkout of the default branch)" >&2
}

tag="${1:-}"
if [ -z "${tag}" ]; then
  usage
  exit 2
fi

# actions/checkout does not fetch tags, so fetch the one tag we were asked about. --depth=1 keeps
# that cheap; the refspec fetches it into the same name it has on the remote.
if ! git rev-parse -q --verify "refs/tags/${tag}^{commit}" >/dev/null 2>&1; then
  if ! git fetch --quiet --depth=1 origin "refs/tags/${tag}:refs/tags/${tag}" 2>/dev/null; then
    echo "error: tag '${tag}' is not in this checkout and could not be fetched from origin." >&2
    echo "       A tag-triggered publish needs the tag to exist on origin." >&2
    exit 1
  fi
fi

tagged="$(git rev-parse "refs/tags/${tag}^{commit}")"
tip="$(git rev-parse HEAD)"

if [ "${tagged}" != "${tip}" ]; then
  cat >&2 <<EOF
error: tag '${tag}' names ${tagged}, but this checkout is ${tip}.
       The publish workflow builds and records the default branch, so a tag can only name that
       tip. Either move the tag to the tip and push it again
         git tag -f ${tag} && git push --force origin ${tag}
       or run the workflow manually from the Actions tab.
EOF
  exit 1
fi

echo "ok: tag '${tag}' names the commit being published (${tip})"
