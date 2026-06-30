#pragma once

#include "acquire.zarr.h" // ZarrStreamSettings

#include <nlohmann/json.hpp>

#include <string>

/**
 * @brief Load and dump ZarrStreamSettings from YAML/JSON config files. JSON is
 * a subset of YAML, so a single parse path handles both.
 */
namespace zarr {
/**
 * @brief Parse a YAML (or JSON) document into JSON.
 * @param text The config document.
 * @return The parsed document as JSON.
 * @throws std::exception on malformed input.
 */
nlohmann::json
config_text_to_json(const std::string& text);

/**
 * @brief Emit a JSON document as YAML text.
 * @param doc The document to serialize.
 * @return The document rendered as YAML.
 */
std::string
json_to_yaml(const nlohmann::json& doc);

/**
 * @brief Build owning stream settings from a config document.
 * @details Allocates all arrays, dimensions, plates, and strings; free with
 * destroy_loaded_settings.
 * @param doc The config document.
 * @param[out] out The stream settings struct to populate.
 * @throws std::exception with a descriptive message on malformed input.
 */
void
json_to_settings(const nlohmann::json& doc, ZarrStreamSettings* out);

/**
 * @brief Serialize stream settings into a config document.
 * @param settings The stream settings to serialize.
 * @return The settings as a JSON document.
 */
nlohmann::json
settings_to_json(const ZarrStreamSettings* settings);

/**
 * @brief Free everything json_to_settings allocated.
 * @param[in, out] settings The loader-populated stream settings struct.
 */
void
destroy_loaded_settings(ZarrStreamSettings* settings);
} // namespace zarr
