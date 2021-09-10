#!/bin/bash -xue

cd $(dirname $0)
export KITE_TITLE='PizzaBox'
export KITE_TAGLINE='Thanks for testing!'
# This file comes from the "secrets" repo.
export KITE_GOOGLE_CLIENT_SECRET=$(cat pizzakite/google-client-secret)
export KITE_GOOGLE_CLIENT_ID='249221604126-4jt7btiej000rogh7nualh4tqn445b7c.apps.googleusercontent.com'
export KITE_GOOGLE_REQUIRED_SUFFIX='@lynxanalytics.com'
export KITE_GOOGLE_HOSTED_DOMAIN='lynxanalytics.com'
export KITE_AUTH_TRUSTED_PUBLIC_KEY_BASE64='MIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEA2jMsw8gu9972/DuKwuh6DwzKU/jikzJ+9WwnhhQqh1pIz330vSo6FRe4Ut/8ge60LjVv+zQhZJRKGQ4kZBBk2TlhBs38quMdrvxmKt382tufo0V3lbqxVWLR8oBXGbkNmKN/8BTk2La4gBGhO75egCr68eGA6swvmixJG8MKid2DjCHZy7TJc5tTE1zmgGQ2cQhkCXr7CzBOzIac1uLhXl94mrjdb9hCnVQ2L2CzsNi6VsR7VCDXuiBAUDwTk0F94VHsTaW/WJ7DY6OgcFayRok6NQ0Eab8qeTvvt701Pqhfc7qblcntq8fSVcIqCt8Sp8UItCgEHXQWLZU727rF3QIDAQAB'
export ZONE='asia-southeast1-a'
export KITE_MASTER_MEMORY_MB=14000
export SPHYNX_CACHED_ENTITIES_MAX_MEM_MB=11000
export KITE_ALLOW_PYTHON='yes'
export SPHYNX_PYTHON_CHROOT='yes'
export NUM_CORES_PER_EXECUTOR=8
./push.sh pizzabox "$@"
