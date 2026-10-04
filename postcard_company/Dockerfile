# Not needed to run this project with dbt. It delivers the project to the
# portable-data-stack-* repos, which build this directory at the tag their
# DATAMART_REF points to and run it on every `docker compose up` with their
# ./dbt/postcard_company bind-mounted at /datamart/postcard_company.
FROM busybox:1.37.0

COPY . /src/postcard_company

# .dockerignore keeps these two out of a local build, but a build from a git
# URL - which is how the stacks build this - does not apply it.
RUN rm -f /src/postcard_company/Dockerfile /src/postcard_company/.dockerignore

# A fingerprint of the project as shipped. The copy runs only when it differs
# from the one the last copy left behind, so moving DATAMART_REF refreshes the
# project, while starting the stack again keeps any edits made there.
RUN cd /src/postcard_company \
 && find . -type f | sort | xargs md5sum | md5sum | cut -d ' ' -f 1 > /src/fingerprint

# The copy overwrites the project files and leaves everything else in the
# directory alone - target/, dbt_packages/, logs/ and any model added locally.
COPY <<'EOF' /usr/local/bin/copy-project
#!/bin/sh
set -eu
dest=/datamart/postcard_company
if cmp -s /src/fingerprint "$dest/.datamart-fingerprint"; then
    echo "$dest already holds this version of the project; leaving it alone."
    exit 0
fi
cp -a /src/postcard_company/. "$dest/"
cp /src/fingerprint "$dest/.datamart-fingerprint"
echo "Copied the dbt project into $dest."
EOF

CMD ["sh", "/usr/local/bin/copy-project"]
