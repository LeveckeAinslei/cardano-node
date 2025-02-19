{ pkgs
, jsonFilePretty

, backend
, profile
, nodeSpecs
, node-services
}:

with pkgs.lib;

let
  # If there is an "explorer" node, the generator will run there!
  # TODO: Repeated code, add the generator's node name to profile.json
  runningNode = if builtins.hasAttr "explorer" nodeSpecs
    then "explorer"
    else "node-0"
  ;

  # We're reusing configuration from a cluster node.
  exemplarNode = node-services."node-0";

  nodePublicIP =
    { i, name, ... }@nodeSpec:
    "127.0.0.1";

  finaliseGeneratorService =
    profile: svc: recursiveUpdate svc
      ({
        sigKey         = "../genesis/utxo-keys/utxo1.skey";
        runScriptFile  = "run-script.json";
        ## path to the config and socket of the locally running node.
        nodeConfigFile = "../${runningNode}/config.json";
        localNodeSocketPath = "../${runningNode}/node.socket";
      } // optionalAttrs profile.node.tracer {
        tracerSocketPath = "../tracer/tracer.socket";
      } // optionalAttrs backend.useCabalRun {
        executable     = "tx-generator";
      });

  finaliseGeneratorConfig =
    cfg: recursiveUpdate cfg
      ({
        AlonzoGenesisFile    = "../genesis/genesis.alonzo.json";
        ShelleyGenesisFile   = "../genesis/genesis-shelley.json";
        ByronGenesisFile     = "../genesis/byron/genesis.json";
        ConwayGenesisFile    = "../genesis/genesis.conway.json";
      } // optionalAttrs backend.useCabalRun {
        executable           = "tx-generator";
      });

  ##
  ## generatorServiceConfig :: Map NodeId NodeSpec -> ServiceConfig
  ##
  generatorServiceConfig =
    nodeSpecs:
    let
      generatorNodeConfigDefault =
        (__fromJSON (__readFile ../../../bench/tx-generator-config-base.json))
        // { inherit (exemplarNode.config.value)
               Protocol
               ByronGenesisFile
               ShelleyGenesisFile
               AlonzoGenesisFile
               ConwayGenesisFile
               ;
           };
    in
        finaliseGeneratorService profile
        {
          inherit (profile) era;

          targetNodes = __mapAttrs
            (name: { name, port, ...}@nodeSpec:
              { inherit port;
                ip = let ip = nodePublicIP nodeSpec; # getPublicIp resources nodes name
                     in __trace "generator target:  ${name}/${ip}:${toString port}" ip;
              })
            (filterAttrs (_: spec: spec.isProducer) nodeSpecs);

          dsmPassthrough = {
            # rtsOpts = ["-xc"];
          };
        }
        //
        ((x: recursiveUpdate x
          { tx_count = __ceil x.tx_count; })
          (removeAttrs profile.generator ["epochs"]));

  ## Given an env config, evaluate it and produce the node service.
  ## Call the given function on this service.
  ##
  ## generatorServiceConfigService :: GeneratorServiceConfig -> GeneratorService
  ##
  generatorServiceConfigService =
    serviceConfig:
    let
    systemdCompat.options = {
      systemd.services = mkOption {};
      systemd.sockets = mkOption {};
      users = mkOption {};
      assertions = mkOption {};
      environment = mkOption {};
    };
    eval = let
      extra = {
        services.tx-generator = {
          enable = true;
        } // serviceConfig;
      };
    in evalModules {
      prefix = [];
      modules = import ../../nixos/module-list.nix
                ++ [ (import ../../nixos/tx-generator-service.nix pkgs)
                     systemdCompat extra
                     { config._module.args = { inherit pkgs; }; }
                   ]
                ++ [ backend.service-modules.generator or {} ];
      # args = { inherit pkgs; };
    };
    in eval.config.services.tx-generator;

  ##
  ## generator-service :: (ServiceConfig, Service, NodeConfig, Script)
  ##
  generator-service =
    (nodeSpecs:
    let
      serviceConfig = generatorServiceConfig nodeSpecs;
      service       = generatorServiceConfigService serviceConfig;
    in {
      start = rec {
        value = ''
          #!${pkgs.stdenv.shell}

          ${service.script}
          '';
        JSON = pkgs.writeScript "startup-generator.sh" value;
      };

      config = rec {
        # TODO / FIXME
        # the string '...' is not allowed to refer to a store path (such as '')
        # value = service.decideRunScript service;
        value = __fromJSON (__readFile JSON);
        JSON  = jsonFilePretty "generator-run-script.json"
                (service.decideRunScript service);
      };
    })
    nodeSpecs;
in
{
  inherit generator-service;
}
