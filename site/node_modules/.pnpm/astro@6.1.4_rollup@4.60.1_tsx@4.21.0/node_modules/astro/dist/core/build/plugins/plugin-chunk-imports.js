import { init, parse } from "es-module-lexer";
import { ASTRO_VITE_ENVIRONMENT_NAMES } from "../../constants.js";
function pluginChunkImports(options) {
  const assetQueryParams = options.settings.adapter?.client?.assetQueryParams;
  if (!assetQueryParams || assetQueryParams.toString() === "") {
    return void 0;
  }
  const queryString = assetQueryParams.toString();
  return {
    name: "@astro/plugin-chunk-imports",
    enforce: "post",
    applyToEnvironment(environment) {
      return environment.name === ASTRO_VITE_ENVIRONMENT_NAMES.client;
    },
    async renderChunk(code, _chunk) {
      if (!code.includes("./")) {
        return null;
      }
      await init;
      const [imports] = parse(code);
      const relativeImports = imports.filter(
        (imp) => imp.n && /^\.\.?\//.test(imp.n) && /\.(?:js|mjs)$/.test(imp.n)
      );
      if (relativeImports.length === 0) {
        return null;
      }
      let rewritten = code;
      for (let i = relativeImports.length - 1; i >= 0; i--) {
        const imp = relativeImports[i];
        rewritten = rewritten.slice(0, imp.e) + "?" + queryString + rewritten.slice(imp.e);
      }
      return { code: rewritten, map: null };
    }
  };
}
export {
  pluginChunkImports
};
