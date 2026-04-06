import {
  isRemoteAllowed,
  matchPattern
} from "@astrojs/internal-helpers/remote";
import { emitClientAsset } from "./assets.js";
import { isESMImportedImage, isRemoteImage, resolveSrc } from "./imageKind.js";
import { imageMetadata } from "./metadata.js";
import { getOrigQueryParams } from "./queryParams.js";
import { inferRemoteSize } from "./remoteProbe.js";
export {
  emitClientAsset,
  getOrigQueryParams,
  imageMetadata,
  inferRemoteSize,
  isESMImportedImage,
  isRemoteAllowed,
  isRemoteImage,
  matchPattern,
  resolveSrc
};
