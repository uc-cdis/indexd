{ pkgs, extraPackages ? [], ... }:
let
  customPackages = [ pkgs.detect-secrets ];
in
{
  packages = customPackages ++ extraPackages;

  git-hooks.hooks = {
    trim-trailing-whitespace.enable = true;
    end-of-file-fixer.enable = true;
    no-commit-to-branch.enable = true;
    no-commit-to-branch.settings.branch = [
      "master"
    ];
    detect-secrets = {
      enable = true;
      entry = ''${pkgs.detect-secrets}/bin/detect-secrets-hook'';
      args = [ "--baseline" ".secrets.baseline" ];
      description = "";
    };
  };
}
