{ pkgs, lib, config, inputs, ... }:
let
  # sharedConfigs = pkgs.fetchFromGitHub {
  #   owner = "uc-cdis";
  #   repo = "devenv-configs";
  #   rev = "main"; # or specific commit hash
  #   sha256 = "sha256-AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA=";
  # };

  # gitHooksConfig = import (sharedConfigs + "./git-hooks.nix") { inherit pkgs; };

  gitHooksConfig = import ./git-hooks.nix {
    inherit pkgs;
    extraPackages = [ pkgs.jq ];
  };
in
lib.recursiveUpdate gitHooksConfig {
  # https://devenv.sh/basics/
  env.GREET = "devenv @ indexd";

  # customizable through git-hooks.nix
  # https://devenv.sh/packages/
  # packages = [
  # ];

  # https://devenv.sh/languages/
  languages.python.enable = true;
  languages.python.version = "3.9";
  languages.python.poetry.enable = true;
  languages.python.poetry.install.enable = true;
  languages.python.poetry.activate.enable = true;
  languages.python.uv.enable = true;

  # https://devenv.sh/processes/
  # processes.cargo-watch.exec = "cargo-watch";

  # https://devenv.sh/services/
  services.postgres.enable = true;
  services.postgres.listen_addresses = "localhost";
  services.postgres.initialDatabases = [
    {
      name = "indexd_tests";
      user = "postgres";
      pass = "postgres";
    }
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
    "indexd:run".exec = "poetry run gunicorn deployment.wsgi.wsgi:application -b 0.0.0.0:8080";
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
  # git-hooks.hooks.trim-trailing-whitespace.enable = true;
  # git-hooks.hooks.end-of-file-fixer.enable = true;
  # git-hooks.hooks.no-commit-to-branch.enable = true;
  # git-hooks.hooks.no-commit-to-branch.settings.branch = [
  #   "master"
  # ];

  # git-hooks.hooks.

  # See full reference at https://devenv.sh/reference/options/
}
