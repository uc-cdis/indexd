{ pkgs, lib, config, inputs, ... }:

{
  # https://devenv.sh/basics/
  env.GREET = "devenv";

  # https://devenv.sh/packages/
  packages = [
    pkgs.detect-secrets
  ];

  # https://devenv.sh/languages/
  languages.python.enable = true;
  languages.python.poetry.enable = true;
  languages.python.poetry.activate.enable = true;
  languages.python.poetry.install.enable = true;
  languages.python.uv.enable = true;
  languages.python.version = "3.9";

  # https://devenv.sh/processes/
  # processes.cargo-watch.exec = "cargo-watch";

  # https://devenv.sh/services/
  services.postgres.enable = true;
  services.postgres.listen_addresses = "localhost";
  services.postgres.initialDatabases = [
    {
      name = "indexd_default";
      user = "indexd";
      pass = "indexd_pAssW0rd1234";
    }
  ];

  # https://devenv.sh/scripts/
  scripts.hello.exec = ''
    echo hello from $GREET
  '';

  scripts.migrate-db.exec = ''
    echo "Create admin user"
    python3 bin/index_admin.py create --username $1 --password $2
    echo "Running migrations"
    cp ./bin/indexd_settings.py ./bin/local_settings.py
    python3 bin/index_admin.py migrate_database
  '';

  scripts.silly-example.exec = ''
    echo $1
  '';

  enterShell = ''
    hello
  '';

  # https://devenv.sh/tasks/
  tasks = {
    "indexd:run".exec = "poetry ";
  #   "devenv:enterShell".after = [ "myproj:setup" ];
  };

  # https://devenv.sh/tests/
  enterTest =
    let
      pg_isready = lib.getExe' config.services.postgres.package "pg_isready";
    in
    ''
      timeout 30 bash -c "until ${pg_isready} -d indexd_default -q; do sleep 0.5; done"
      migrate-db admin admin
      echo "Running tests"
      poetry run pytest -vv --cov=indexd --cov-report xml tests/
    '';

  # https://devenv.sh/git-hooks/
  git-hooks.hooks.trim-trailing-whitespace.enable = true;
  git-hooks.hooks.end-of-file-fixer.enable = true;
  git-hooks.hooks.no-commit-to-branch.enable = true;
  git-hooks.hooks.no-commit-to-branch.settings.branch = [
    "master"
  ];

  git-hooks.hooks.detect-secrets = {
    enable = true;
    entry = ''${pkgs.detect-secrets}/bin/detect-secrets-hook'';
    args = [ "--baseline" ".secrets.baseline" ];
    description = "";
  };

  # See full reference at https://devenv.sh/reference/options/
}
