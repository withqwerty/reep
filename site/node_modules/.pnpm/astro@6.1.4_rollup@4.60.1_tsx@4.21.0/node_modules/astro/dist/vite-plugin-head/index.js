import { hasHeadInjectComment } from "../core/head-propagation/comment.js";
import {
  buildImporterGraphFromModuleInfo,
  computeInTreeAncestors
} from "../core/head-propagation/graph.js";
import { getTopLevelPageModuleInfos } from "../core/build/graph.js";
import { getAstroMetadata } from "../vite-plugin-astro/index.js";
import { ASTRO_VITE_ENVIRONMENT_NAMES } from "../core/constants.js";
const VIRTUAL_COMPONENT_METADATA = "virtual:astro:component-metadata";
const RESOLVED_VIRTUAL_COMPONENT_METADATA = `\0${VIRTUAL_COMPONENT_METADATA}`;
function configHeadVitePlugin() {
  let environment;
  function invalidateComponentMetadataModule() {
    const virtualMod = environment.moduleGraph.getModuleById(RESOLVED_VIRTUAL_COMPONENT_METADATA);
    if (virtualMod) {
      environment.moduleGraph.invalidateModule(virtualMod);
    }
  }
  function buildImporterGraphFromEnvironment(seed) {
    const queue = [seed];
    const collected = /* @__PURE__ */ new Set();
    while (queue.length > 0) {
      const current = queue.pop();
      if (collected.has(current)) continue;
      collected.add(current);
      const mod = environment.moduleGraph.getModuleById(current);
      for (const importer of mod?.importers ?? []) {
        if (importer.id) {
          queue.push(importer.id);
        }
      }
    }
    return buildImporterGraphFromModuleInfo(collected, (id) => {
      const mod = environment.moduleGraph.getModuleById(id);
      if (!mod) return null;
      return {
        importers: Array.from(mod.importers).map((importer) => importer.id).filter((moduleId) => !!moduleId),
        dynamicImporters: []
      };
    });
  }
  function propagateMetadata(seed, prop, value) {
    const importerGraph = buildImporterGraphFromEnvironment(seed);
    const allAncestors = computeInTreeAncestors({
      seeds: [seed],
      importerGraph
    });
    for (const id of allAncestors) {
      const info = this.getModuleInfo(id);
      if (info?.meta.astro) {
        const astroMetadata = getAstroMetadata(info);
        if (astroMetadata) {
          Reflect.set(astroMetadata, prop, value);
        }
      }
    }
    invalidateComponentMetadataModule();
  }
  return {
    name: "astro:head-metadata",
    enforce: "pre",
    apply: "serve",
    configureServer(devServer) {
      environment = devServer.environments[ASTRO_VITE_ENVIRONMENT_NAMES.ssr];
      devServer.watcher.on("add", invalidateComponentMetadataModule);
      devServer.watcher.on("unlink", invalidateComponentMetadataModule);
      devServer.watcher.on("change", invalidateComponentMetadataModule);
    },
    load(id) {
      if (id !== RESOLVED_VIRTUAL_COMPONENT_METADATA) {
        return;
      }
      const componentMetadataEntries = [];
      for (const [moduleId, mod] of environment.moduleGraph.idToModuleMap) {
        const info = this.getModuleInfo(moduleId) ?? (mod.id ? this.getModuleInfo(mod.id) : null);
        if (!info) continue;
        const astro = getAstroMetadata(info);
        if (!astro) continue;
        componentMetadataEntries.push([
          moduleId,
          {
            containsHead: astro.containsHead,
            propagation: astro.propagation
          }
        ]);
      }
      return {
        code: `export const componentMetadataEntries = ${JSON.stringify(componentMetadataEntries)};`
      };
    },
    resolveId(source, importer) {
      if (source === VIRTUAL_COMPONENT_METADATA) {
        return RESOLVED_VIRTUAL_COMPONENT_METADATA;
      }
      if (importer) {
        return this.resolve(source, importer, { skipSelf: true }).then((result) => {
          if (result) {
            let info = this.getModuleInfo(result.id);
            const astro = info && getAstroMetadata(info);
            if (astro) {
              if (astro.propagation === "self" || astro.propagation === "in-tree") {
                propagateMetadata.call(this, importer, "propagation", "in-tree");
              }
              if (astro.containsHead) {
                propagateMetadata.call(this, importer, "containsHead", true);
              }
            }
          }
          return result;
        });
      }
    },
    transform(source, id) {
      let info = this.getModuleInfo(id);
      if (info && getAstroMetadata(info)?.containsHead) {
        propagateMetadata.call(this, id, "containsHead", true);
      }
      if (hasHeadInjectComment(source)) {
        propagateMetadata.call(this, id, "propagation", "in-tree");
      }
      invalidateComponentMetadataModule();
    }
  };
}
function astroHeadBuildPlugin(internals) {
  return {
    name: "astro:head-metadata-build",
    applyToEnvironment(environment) {
      return environment.name === ASTRO_VITE_ENVIRONMENT_NAMES.ssr || environment.name === ASTRO_VITE_ENVIRONMENT_NAMES.prerender;
    },
    generateBundle(_opts, bundle) {
      const map = internals.componentMetadata;
      const moduleIds = /* @__PURE__ */ new Set();
      const selfPropagationSeeds = /* @__PURE__ */ new Set();
      const commentPropagationSeeds = /* @__PURE__ */ new Set();
      function getOrCreateMetadata(id) {
        if (map.has(id)) return map.get(id);
        const metadata = {
          propagation: "none",
          containsHead: false
        };
        map.set(id, metadata);
        return metadata;
      }
      for (const [, output] of Object.entries(bundle)) {
        if (output.type !== "chunk") continue;
        for (const [id, mod] of Object.entries(output.modules)) {
          moduleIds.add(id);
          const modinfo = this.getModuleInfo(id);
          if (modinfo) {
            const meta = getAstroMetadata(modinfo);
            if (meta?.containsHead) {
              for (const pageInfo of getTopLevelPageModuleInfos(id, this)) {
                let metadata = getOrCreateMetadata(pageInfo.id);
                metadata.containsHead = true;
              }
            }
            if (meta?.propagation === "self") {
              selfPropagationSeeds.add(id);
            }
          }
          if (mod.code && hasHeadInjectComment(mod.code)) {
            commentPropagationSeeds.add(id);
          }
        }
      }
      const importerGraph = buildImporterGraphFromModuleInfo(
        moduleIds,
        (id) => this.getModuleInfo(id)
      );
      const allPropagationSeeds = /* @__PURE__ */ new Set([...selfPropagationSeeds, ...commentPropagationSeeds]);
      const allAncestors = computeInTreeAncestors({
        seeds: allPropagationSeeds,
        importerGraph
      });
      for (const id of allAncestors) {
        const metadata = getOrCreateMetadata(id);
        if (metadata.propagation !== "self") {
          metadata.propagation = "in-tree";
        }
      }
    }
  };
}
export {
  astroHeadBuildPlugin,
  configHeadVitePlugin as default
};
