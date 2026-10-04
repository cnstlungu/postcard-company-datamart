# Not needed to run this project with dbt. It delivers the project to the
# portable-data-stack-* repos, which build this directory at the tag their
# DATAMART_REF points to and run it once at `docker compose up` with their
# ./dbt/postcard_company bind-mounted at /datamart/postcard_company.
#
# The copy overwrites the project files from the tag and leaves everything else
# in that directory alone - target/, dbt_packages/, logs/ and any model added
# locally - which is what the git checkout it replaces did.
FROM busybox:1.37.0

COPY . /src/postcard_company

CMD ["cp", "-a", "/src/postcard_company/.", "/datamart/postcard_company/"]
