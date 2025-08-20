// Copyright 2025 Memgraph Ltd.
//
// Use of this software is governed by the Business Source License
// included in the file licenses/BSL.txt; by using this file, you agree to be bound by the terms of the Business Source
// License, and you may not use this file except in compliance with the Business Source License.
//
// As of the Change Date specified in that file, in accordance with
// the Business Source License, use of this software will be governed
// by the Apache License, Version 2.0, included in the file
// licenses/APL.txt.

#include <ctype.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include "mg_procedure.h"

// Constants
#define MAX_STRING_LENGTH 256
#define MAX_PROPERTIES 16

// Function names
static const char *kProcedureToTree = "to_tree";

// Parameter names
static const char *kParameterPaths = "paths";
static const char *kParameterConfig = "config";
static const char *kParameterLowerCaseRels = "lowerCaseRels";

// Return value name
static const char *kReturnValue = "value";

// Filter modes
typedef enum { FILTER_MODE_INCLUDE, FILTER_MODE_EXCLUDE, FILTER_MODE_WILDCARD, FILTER_MODE_INVALID } filter_mode_t;

// Generic property filter structure
typedef struct {
  filter_mode_t mode;
  char properties[MAX_PROPERTIES][MAX_STRING_LENGTH];
  size_t property_count;
} property_filter_t;

// Node filter config structure
typedef struct {
  size_t filter_count;
  property_filter_t filters[MAX_PROPERTIES];
  char labels[MAX_PROPERTIES][MAX_STRING_LENGTH];
} node_property_filter_config_t;

// Relationship filter config structure
typedef struct {
  size_t filter_count;
  property_filter_t filters[MAX_PROPERTIES];
  char rel_types[MAX_PROPERTIES][MAX_STRING_LENGTH];
} rel_property_filter_config_t;

// Global filter config structure
typedef struct {
  node_property_filter_config_t node_config;
  rel_property_filter_config_t rel_config;
  bool has_node_config;
  bool has_rel_config;
} filter_config_t;

// Safe string length function
static size_t safe_strlen(const char *str) { return str ? strlen(str) : 0; }

// Safe string copy function
static bool safe_strcpy(char *dest, const char *src, size_t dest_size) {
  if (!dest || !src || dest_size == 0) {
    return false;
  }

  size_t src_len = safe_strlen(src);
  if (src_len >= dest_size) {
    return false;  // Would overflow
  }

  strcpy(dest, src);
  return true;
}

// Safe string copy with truncation
static bool safe_strcpy_truncate(char *dest, const char *src, size_t dest_size) {
  if (!dest || !src || dest_size == 0) {
    return false;
  }

  size_t src_len = safe_strlen(src);
  if (src_len >= dest_size) {
    // Truncate to fit
    strncpy(dest, src, dest_size - 1);
    dest[dest_size - 1] = '\0';
  } else {
    strcpy(dest, src);
  }
  return true;
}

// Safe string copy with dash removal
static bool safe_strcpy_remove_dash(char *dest, const char *src, size_t dest_size) {
  if (!dest || !src || dest_size == 0) {
    return false;
  }

  size_t src_len = safe_strlen(src);
  if (src_len == 0) {
    return false;
  }

  // Skip leading dash if present
  const char *start = (src[0] == '-') ? src + 1 : src;
  size_t actual_len = safe_strlen(start);

  if (actual_len >= dest_size) {
    // Truncate to fit
    strncpy(dest, start, dest_size - 1);
    dest[dest_size - 1] = '\0';
  } else {
    strcpy(dest, start);
  }
  return true;
}

// Helper function to convert string to lowercase
static char *to_lowercase(const char *str, struct mgp_memory *memory) {
  if (!str) return NULL;
  size_t len = safe_strlen(str);
  void *ptr;
  // Align to 64 bytes
  size_t alloc_size = len + 1;
  alloc_size = (alloc_size + 63) & ~63ULL;
  if (mgp_alloc(memory, alloc_size, &ptr) != MGP_ERROR_NO_ERROR) {
    return NULL;
  }
  char *lower = (char *)ptr;
  for (size_t i = 0; i < len; i++) {
    lower[i] = tolower(str[i]);
  }
  lower[len] = '\0';
  return lower;
}

// Helper function to concatenate strings
static char *concatenate_strings(const char *str1, const char *str2, struct mgp_memory *memory) {
  if (!str1 || !str2) return NULL;
  size_t len1 = safe_strlen(str1);
  size_t len2 = safe_strlen(str2);
  void *ptr = NULL;
  // Align to 64 bytes
  size_t alloc_size = len1 + len2 + 1;
  alloc_size = (alloc_size + 63) & ~63ULL;
  if (mgp_alloc(memory, alloc_size, &ptr) != MGP_ERROR_NO_ERROR || ptr == NULL) {
    return NULL;
  }
  char *result = (char *)ptr;

  // Safe string operations
  if (!safe_strcpy(result, str1, alloc_size)) {
    mgp_free(memory, ptr);
    return NULL;
  }

  if (!safe_strcpy(result + len1, str2, alloc_size - len1)) {
    mgp_free(memory, ptr);
    return NULL;
  }

  return result;
}

static inline uint64_t fnv1a64(const void *data, size_t len, uint64_t seed) {
  const uint8_t *p = (const uint8_t *)data;
  uint64_t h = 1469598103934665603ULL ^ seed;
  for (size_t i = 0; i < len; ++i) {
    h ^= p[i];
    h *= 1099511628211ULL;
  }
  return h;
}

// Helper function to check if a string is in the properties array
static bool property_in_list(const char *prop_name, const char properties[][MAX_STRING_LENGTH], size_t count) {
  if (!prop_name) {
    return false;
  }

  for (size_t i = 0; i < count; i++) {
    if (strcmp(prop_name, properties[i]) == 0) {
      return true;
    }
  }
  return false;
}

// Initialize property filter
static void init_property_filter(property_filter_t *filter) {
  filter->mode = FILTER_MODE_INVALID;
  filter->property_count = 0;
}

// Parse property filter from list
static void parse_property_filter(property_filter_t *filter, struct mgp_list *props) {
  init_property_filter(filter);

  size_t list_size;
  if (mgp_list_size(props, &list_size) != MGP_ERROR_NO_ERROR || list_size == 0) {
    return;
  }

  // Check if all entries are strings
  for (size_t i = 0; i < list_size; i++) {
    struct mgp_value *item;
    if (mgp_list_at(props, i, &item) != MGP_ERROR_NO_ERROR) {
      return;
    }
    enum mgp_value_type item_type;
    if (mgp_value_get_type(item, &item_type) != MGP_ERROR_NO_ERROR || item_type != MGP_VALUE_TYPE_STRING) {
      return;
    }
  }

  // Get first item to determine mode
  struct mgp_value *first_item;
  if (mgp_list_at(props, 0, &first_item) != MGP_ERROR_NO_ERROR) {
    return;
  }
  const char *first_str;
  if (mgp_value_get_string(first_item, &first_str) != MGP_ERROR_NO_ERROR) {
    return;
  }

  if (strcmp(first_str, "*") == 0) {
    filter->mode = FILTER_MODE_WILDCARD;
    return;
  }

  if (first_str[0] == '-') {
    filter->mode = FILTER_MODE_EXCLUDE;
    for (size_t i = 0; i < list_size && i < MAX_PROPERTIES; i++) {
      struct mgp_value *item;
      if (mgp_list_at(props, i, &item) == MGP_ERROR_NO_ERROR) {
        const char *val_str;
        if (mgp_value_get_string(item, &val_str) == MGP_ERROR_NO_ERROR) {
          if (safe_strcpy_remove_dash(filter->properties[filter->property_count], val_str, MAX_STRING_LENGTH)) {
            filter->property_count++;
          }
        }
      }
    }
    return;
  }

  // Inclusion mode
  filter->mode = FILTER_MODE_INCLUDE;
  for (size_t i = 0; i < list_size && i < MAX_PROPERTIES; i++) {
    struct mgp_value *item;
    if (mgp_list_at(props, i, &item) == MGP_ERROR_NO_ERROR) {
      const char *val_str;
      if (mgp_value_get_string(item, &val_str) == MGP_ERROR_NO_ERROR) {
        if (safe_strcpy_truncate(filter->properties[filter->property_count], val_str, MAX_STRING_LENGTH)) {
          filter->property_count++;
        }
      }
    }
  }
}

// Get node property filter for a given label
static const property_filter_t *get_node_property_filter(const node_property_filter_config_t *config,
                                                         const char *label) {
  if (!config) {
    return NULL;
  }

  for (size_t i = 0; i < config->filter_count; i++) {
    if (strcmp(config->labels[i], label) == 0) {
      return &config->filters[i];
    }
  }

  // Return default wildcard filter if not found
  static property_filter_t default_wildcard = {FILTER_MODE_WILDCARD, {{0}}, 0};
  return &default_wildcard;
}

// Check if node property should be included
static bool should_include_node_property(const char *prop_name, const filter_config_t *filter_config,
                                         struct mgp_vertex *node) {
  if (!filter_config || !filter_config->has_node_config || !node) {
    return true;  // No filtering config or invalid node: include all properties
  }

  // Get node labels with proper validation
  size_t label_count = 0;
  if (mgp_vertex_labels_count(node, &label_count) != MGP_ERROR_NO_ERROR) {
    return true;
  }

  property_filter_t filters[MAX_PROPERTIES];
  size_t filter_count = 0;
  bool has_invalid_filter = false;

  // Check each label for filters
  for (size_t label_idx = 0; label_idx < label_count; label_idx++) {
    struct mgp_label label = {.name = NULL};
    if (mgp_vertex_label_at(node, label_idx, &label) != MGP_ERROR_NO_ERROR || !label.name) {
      continue;  // Skip invalid labels
    }

    const property_filter_t *filter = get_node_property_filter(&filter_config->node_config, label.name);
    if (!filter) continue;
    if (filter->mode != FILTER_MODE_INVALID && filter_count < MAX_PROPERTIES) {
      filters[filter_count] = *filter;
      filter_count++;
    } else if (filter->mode == FILTER_MODE_INVALID) {
      has_invalid_filter = true;
    }
  }

  // If we have invalid filters, exclude all properties for affected labels
  if (has_invalid_filter && filter_count == 0) {
    return false;
  }
  if (filter_count == 0) {
    return true;
  }

  // Exclusion mode takes precedence if any filter is exclusion
  for (size_t i = 0; i < filter_count; i++) {
    if (filters[i].mode == FILTER_MODE_EXCLUDE) {
      return !property_in_list(prop_name, filters[i].properties, filters[i].property_count);
    }
  }

  // Wildcard mode if any filter is wildcard
  for (size_t i = 0; i < filter_count; i++) {
    if (filters[i].mode == FILTER_MODE_WILDCARD) {
      return true;
    }
  }

  // Otherwise, inclusion mode: include if any filter includes the property
  for (size_t i = 0; i < filter_count; i++) {
    if (filters[i].mode == FILTER_MODE_INCLUDE &&
        property_in_list(prop_name, filters[i].properties, filters[i].property_count)) {
      return true;
    }
  }

  return false;
}

// Initialize node property filter config
static void init_node_property_filter_config(node_property_filter_config_t *config) { config->filter_count = 0; }

// Initialize relationship property filter config
static void init_rel_property_filter_config(rel_property_filter_config_t *config) { config->filter_count = 0; }

// Initialize global filter config
static void init_filter_config(filter_config_t *config) {
  init_node_property_filter_config(&config->node_config);
  init_rel_property_filter_config(&config->rel_config);
  config->has_node_config = false;
  config->has_rel_config = false;
}

// Parse node property filter config from map
static void parse_node_property_filter_config(node_property_filter_config_t *config, struct mgp_map *nodes_map,
                                              struct mgp_memory *memory) {
  init_node_property_filter_config(config);

  if (!nodes_map) {
    return;
  }

  struct mgp_map_items_iterator *iter = NULL;
  if (mgp_map_iter_items(nodes_map, memory, &iter) != MGP_ERROR_NO_ERROR || iter == NULL) {
    return;
  }

  struct mgp_map_item *item;
  if (mgp_map_items_iterator_get(iter, &item) == MGP_ERROR_NO_ERROR && item != NULL) {
    do {
      const char *key_str;
      if (mgp_map_item_key(item, &key_str) != MGP_ERROR_NO_ERROR || key_str == NULL) {
        continue;
      }

      struct mgp_value *value;
      if (mgp_map_item_value(item, &value) != MGP_ERROR_NO_ERROR || value == NULL) {
        continue;
      }

      enum mgp_value_type value_type;
      if (mgp_value_get_type(value, &value_type) != MGP_ERROR_NO_ERROR || value_type != MGP_VALUE_TYPE_LIST) {
        continue;
      }

      struct mgp_list *props_list;
      if (mgp_value_get_list(value, &props_list) != MGP_ERROR_NO_ERROR) {
        continue;
      }

      if (config->filter_count < MAX_PROPERTIES) {
        if (safe_strcpy_truncate(config->labels[config->filter_count], key_str, MAX_STRING_LENGTH)) {
          parse_property_filter(&config->filters[config->filter_count], props_list);
          config->filter_count++;
        }
      }
    } while (mgp_map_items_iterator_next(iter, &item) == MGP_ERROR_NO_ERROR && item != NULL);
  }
  mgp_map_items_iterator_destroy(iter);
}

// Parse relationship property filter config from map
static void parse_rel_property_filter_config(rel_property_filter_config_t *config, struct mgp_map *rels_map,
                                             struct mgp_memory *memory) {
  init_rel_property_filter_config(config);

  if (!rels_map) {
    return;
  }

  struct mgp_map_items_iterator *iter = NULL;
  if (mgp_map_iter_items(rels_map, memory, &iter) != MGP_ERROR_NO_ERROR || iter == NULL) {
    return;
  }

  struct mgp_map_item *item;
  if (mgp_map_items_iterator_get(iter, &item) == MGP_ERROR_NO_ERROR && item != NULL) {
    do {
      const char *key_str;
      if (mgp_map_item_key(item, &key_str) != MGP_ERROR_NO_ERROR || key_str == NULL) {
        continue;
      }

      struct mgp_value *value;
      if (mgp_map_item_value(item, &value) != MGP_ERROR_NO_ERROR || value == NULL) {
        continue;
      }

      enum mgp_value_type value_type;
      if (mgp_value_get_type(value, &value_type) != MGP_ERROR_NO_ERROR || value_type != MGP_VALUE_TYPE_LIST) {
        continue;
      }

      struct mgp_list *props_list;
      if (mgp_value_get_list(value, &props_list) != MGP_ERROR_NO_ERROR) {
        continue;
      }

      if (config->filter_count < MAX_PROPERTIES) {
        if (safe_strcpy_truncate(config->rel_types[config->filter_count], key_str, MAX_STRING_LENGTH)) {
          parse_property_filter(&config->filters[config->filter_count], props_list);
          config->filter_count++;
        }
      }
    } while (mgp_map_items_iterator_next(iter, &item) == MGP_ERROR_NO_ERROR && item != NULL);
  }
  mgp_map_items_iterator_destroy(iter);
}

// Parse entire filter config once
static void parse_filter_config(filter_config_t *filter_config, struct mgp_map *config, struct mgp_memory *memory) {
  init_filter_config(filter_config);

  if (!config) {
    return;
  }

  // Parse node filters
  struct mgp_value *nodes_value = NULL;
  if (mgp_map_at(config, "nodes", &nodes_value) == MGP_ERROR_NO_ERROR && nodes_value != NULL) {
    enum mgp_value_type nodes_type = MGP_VALUE_TYPE_NULL;
    if (mgp_value_get_type(nodes_value, &nodes_type) == MGP_ERROR_NO_ERROR && nodes_type == MGP_VALUE_TYPE_MAP) {
      struct mgp_map *nodes_map = NULL;
      if (mgp_value_get_map(nodes_value, &nodes_map) == MGP_ERROR_NO_ERROR && nodes_map != NULL) {
        parse_node_property_filter_config(&filter_config->node_config, nodes_map, memory);
        filter_config->has_node_config = true;
      }
    }
  }

  // Parse relationship filters
  struct mgp_value *rels_value = NULL;
  if (mgp_map_at(config, "rels", &rels_value) == MGP_ERROR_NO_ERROR && rels_value != NULL) {
    enum mgp_value_type rels_type = MGP_VALUE_TYPE_NULL;
    if (mgp_value_get_type(rels_value, &rels_type) == MGP_ERROR_NO_ERROR && rels_type == MGP_VALUE_TYPE_MAP) {
      struct mgp_map *rels_map = NULL;
      if (mgp_value_get_map(rels_value, &rels_map) == MGP_ERROR_NO_ERROR && rels_map != NULL) {
        parse_rel_property_filter_config(&filter_config->rel_config, rels_map, memory);
        filter_config->has_rel_config = true;
      }
    }
  }
}

// Get relationship property filter for a given relationship type
static const property_filter_t *get_rel_property_filter(const rel_property_filter_config_t *config,
                                                        const char *rel_type) {
  if (!config) {
    return NULL;
  }

  for (size_t i = 0; i < config->filter_count; i++) {
    if (strcmp(config->rel_types[i], rel_type) == 0) {
      return &config->filters[i];
    }
  }

  // Return default wildcard filter if not found
  static property_filter_t default_wildcard = {FILTER_MODE_WILDCARD, {{0}}, 0};
  return &default_wildcard;
}

// Check if relationship property should be included
static bool should_include_rel_property(const char *prop_name, const filter_config_t *filter_config,
                                        const char *rel_type) {
  if (!filter_config || !filter_config->has_rel_config) {
    return true;  // No filtering config: include all properties
  }

  const property_filter_t *filter = get_rel_property_filter(&filter_config->rel_config, rel_type);

  if (!filter) {
    return true;
  }

  switch (filter->mode) {
    case FILTER_MODE_WILDCARD:
      return true;
    case FILTER_MODE_EXCLUDE:
      return !property_in_list(prop_name, filter->properties, filter->property_count);
    case FILTER_MODE_INCLUDE:
      return property_in_list(prop_name, filter->properties, filter->property_count);
    default:
      return false;
  }
}

static void merge_trees(struct mgp_map *target, struct mgp_map *source, struct mgp_memory *memory);

typedef struct {
  int64_t id;       /* required */
  const char *type; /* required */
  bool valid;
} merge_identity_t;

/* Extracts (_id:int, _type:string) from a node/edge map item.
   Returns .valid=false if anything is missing or types mismatch. */
static inline merge_identity_t extract_identity(struct mgp_map *map) {
  merge_identity_t out = {.id = 0, .type = NULL, .valid = false};
  if (!map) return out;

  struct mgp_value *id_v = NULL, *type_v = NULL;
  if (mgp_map_at(map, "_id", &id_v) != MGP_ERROR_NO_ERROR || id_v == NULL) return out;
  if (mgp_map_at(map, "_type", &type_v) != MGP_ERROR_NO_ERROR || type_v == NULL) return out;

  enum mgp_value_type t_id = MGP_VALUE_TYPE_NULL, t_type = MGP_VALUE_TYPE_NULL;
  if (mgp_value_get_type(id_v, &t_id) != MGP_ERROR_NO_ERROR || t_id != MGP_VALUE_TYPE_INT) return out;
  if (mgp_value_get_type(type_v, &t_type) != MGP_ERROR_NO_ERROR || t_type != MGP_VALUE_TYPE_STRING) return out;

  int64_t id;
  const char *type_s = NULL;
  if (mgp_value_get_int(id_v, &id) != MGP_ERROR_NO_ERROR) return out;
  if (mgp_value_get_string(type_v, &type_s) != MGP_ERROR_NO_ERROR || !type_s) return out;

  out.id = id;
  out.type = type_s;
  out.valid = true;
  return out;
}

typedef struct {
  int64_t id;
  const char *type;
  struct mgp_map *map; /* value map in the list */
  bool used;
} bucket_t;

/* Insert-or-find probe on open addressing table */
static inline ssize_t probe_find_or_empty(bucket_t *tbl, size_t cap, int64_t id, const char *type, uint64_t h) {
  size_t mask = cap - 1;
  size_t pos = (size_t)h & mask;
  while (true) {
    if (!tbl[pos].used) return (ssize_t)pos;                   /* empty slot */
    if (tbl[pos].id == id && strcmp(tbl[pos].type, type) == 0) /* match */
      return (ssize_t)pos;
    pos = (pos + 1) & mask;
  }
}

/* Build a hash table over existing_list’s map items keyed by (_id,_type). cap is power-of-two. */
static bool index_existing_children(struct mgp_list *existing_list, bucket_t *tbl, size_t cap) {
  size_t n = 0;
  if (mgp_list_size(existing_list, &n) != MGP_ERROR_NO_ERROR) return false;
  for (size_t i = 0; i < n; ++i) {
    struct mgp_value *item_v = NULL;
    if (mgp_list_at(existing_list, i, &item_v) != MGP_ERROR_NO_ERROR || !item_v) continue;
    enum mgp_value_type t = MGP_VALUE_TYPE_NULL;
    if (mgp_value_get_type(item_v, &t) != MGP_ERROR_NO_ERROR || t != MGP_VALUE_TYPE_MAP) continue;
    struct mgp_map *m = NULL;
    if (mgp_value_get_map(item_v, &m) != MGP_ERROR_NO_ERROR || !m) continue;

    merge_identity_t id = extract_identity(m);
    if (!id.valid) continue;

    /* Hash combine: hash(type) then mix in id */
    uint64_t h = fnv1a64(id.type, strlen(id.type), 0);
    h = fnv1a64(&id.id, sizeof(id.id), h);

    ssize_t pos = probe_find_or_empty(tbl, cap, id.id, id.type, h);
    if (pos < 0) return false;
    if (!tbl[pos].used) {
      tbl[pos].used = true;
      tbl[pos].id = id.id;
      tbl[pos].type = id.type;
      tbl[pos].map = m;
    }
    /* if it already existed, we ignore duplicates in existing_list */
  }
  return true;
}

/* Merges new_list into existing_list by hashing existing_list.
   Average O(n) with low constants; preserves old behavior. */
static void merge_list_into_list_hashed(struct mgp_list *existing_list, struct mgp_list *new_list,
                                        struct mgp_memory *memory) {
  size_t exist_n = 0, new_n = 0;
  if (mgp_list_size(existing_list, &exist_n) != MGP_ERROR_NO_ERROR) return;
  if (mgp_list_size(new_list, &new_n) != MGP_ERROR_NO_ERROR) return;
  if (new_n == 0) return;

  /* Capacity: next power of two >= 2*exist_n (at least 8). */
  size_t cap = 8;
  while (cap < (exist_n << 1)) cap <<= 1;

  bucket_t *tbl = NULL;
  size_t bytes = cap * sizeof(bucket_t);
  void *ptr = NULL;
  if (mgp_alloc(memory, bytes, &ptr) != MGP_ERROR_NO_ERROR || !ptr) {
    /* Fallback to original O(n²) merge if allocation fails */
    for (size_t i = 0; i < new_n; i++) {
      struct mgp_value *new_item = NULL;
      if (mgp_list_at(new_list, i, &new_item) != MGP_ERROR_NO_ERROR || !new_item) continue;
      bool merged = false;

      enum mgp_value_type tnew = MGP_VALUE_TYPE_NULL;
      if (mgp_value_get_type(new_item, &tnew) != MGP_ERROR_NO_ERROR || tnew != MGP_VALUE_TYPE_MAP) {
        mgp_list_append_move(existing_list, new_item);
        continue;
      }
      struct mgp_map *new_map = NULL;
      if (mgp_value_get_map(new_item, &new_map) != MGP_ERROR_NO_ERROR || !new_map) {
        mgp_list_append_move(existing_list, new_item);
        continue;
      }
      merge_identity_t nid = extract_identity(new_map);

      for (size_t j = 0; j < exist_n; j++) {
        struct mgp_value *old_item = NULL;
        if (mgp_list_at(existing_list, j, &old_item) != MGP_ERROR_NO_ERROR || !old_item) continue;
        enum mgp_value_type told = MGP_VALUE_TYPE_NULL;
        if (mgp_value_get_type(old_item, &told) != MGP_ERROR_NO_ERROR || told != MGP_VALUE_TYPE_MAP) continue;
        struct mgp_map *old_map = NULL;
        if (mgp_value_get_map(old_item, &old_map) != MGP_ERROR_NO_ERROR || !old_map) continue;

        merge_identity_t oid = extract_identity(old_map);
        if (nid.valid && oid.valid && nid.id == oid.id && strcmp(nid.type, oid.type) == 0) {
          merge_trees(old_map, new_map, memory);
          merged = true;
          break;
        }
      }
      if (!merged) mgp_list_append_move(existing_list, new_item);
    }
    return;
  }

  tbl = (bucket_t *)ptr;
  memset(tbl, 0, bytes);
  if (!index_existing_children(existing_list, tbl, cap)) {
    mgp_free(memory, tbl);
    return;
  }

  /* For each new child: merge or append, updating the index when appending. */
  for (size_t i = 0; i < new_n; ++i) {
    struct mgp_value *new_item = NULL;
    if (mgp_list_at(new_list, i, &new_item) != MGP_ERROR_NO_ERROR || !new_item) continue;

    enum mgp_value_type tnew = MGP_VALUE_TYPE_NULL;
    if (mgp_value_get_type(new_item, &tnew) != MGP_ERROR_NO_ERROR) continue;

    if (tnew == MGP_VALUE_TYPE_MAP) {
      struct mgp_map *new_map = NULL;
      if (mgp_value_get_map(new_item, &new_map) != MGP_ERROR_NO_ERROR || !new_map) {
        mgp_list_append_move(existing_list, new_item);
        continue;
      }

      merge_identity_t nid = extract_identity(new_map);
      if (!nid.valid) {
        mgp_list_append_move(existing_list, new_item);
        continue;
      }

      uint64_t h = fnv1a64(nid.type, strlen(nid.type), 0);
      h = fnv1a64(&nid.id, sizeof(nid.id), h);

      ssize_t pos = probe_find_or_empty(tbl, cap, nid.id, nid.type, h);
      if (pos >= 0 && tbl[pos].used) {
        /* Merge into existing */
        merge_trees(tbl[pos].map, new_map, memory);
      } else {
        /* Append and index */
        /* NOTE: we need the map pointer after append, so keep it now. */
        mgp_list_append_move(existing_list, new_item);
        tbl[pos].used = true;
        tbl[pos].id = nid.id;
        tbl[pos].type = nid.type;
        tbl[pos].map = new_map;
      }
    } else {
      /* Non-map items: just append, as before. */
      mgp_list_append_move(existing_list, new_item);
    }
  }

  mgp_free(memory, tbl);
}

static void merge_trees(struct mgp_map *target, struct mgp_map *source, struct mgp_memory *memory) {
  struct mgp_map_items_iterator *iter;
  if (mgp_map_iter_items(source, memory, &iter) != MGP_ERROR_NO_ERROR || !iter) return;

  struct mgp_map_item *item;
  if (mgp_map_items_iterator_get(iter, &item) == MGP_ERROR_NO_ERROR && item) {
    do {
      const char *key_str = NULL;
      if (mgp_map_item_key(item, &key_str) != MGP_ERROR_NO_ERROR || !key_str) continue;

      /* Skip metadata keys */
      if (key_str[0] == '_' && (strcmp(key_str, "_type") == 0 || strcmp(key_str, "_id") == 0)) continue;

      struct mgp_value *src_val = NULL;
      if (mgp_map_item_value(item, &src_val) != MGP_ERROR_NO_ERROR || !src_val) continue;

      struct mgp_value *dst_val = NULL;
      if (mgp_map_at(target, key_str, &dst_val) == MGP_ERROR_NO_ERROR && dst_val) {
        enum mgp_value_type td = MGP_VALUE_TYPE_NULL, ts = MGP_VALUE_TYPE_NULL;
        if (mgp_value_get_type(dst_val, &td) == MGP_ERROR_NO_ERROR &&
            mgp_value_get_type(src_val, &ts) == MGP_ERROR_NO_ERROR && td == MGP_VALUE_TYPE_LIST &&
            ts == MGP_VALUE_TYPE_LIST) {
          struct mgp_list *dst_list = NULL, *src_list = NULL;
          if (mgp_value_get_list(dst_val, &dst_list) == MGP_ERROR_NO_ERROR &&
              mgp_value_get_list(src_val, &src_list) == MGP_ERROR_NO_ERROR && dst_list && src_list) {
            /* Optimized merge: hash-based */
            merge_list_into_list_hashed(dst_list, src_list, memory);
          }
        } else {
          /* If types mismatch or not lists, last one wins (keep old behavior: overwrite by insert_move?) */
          /* We mimic original intent: when key exists and both are lists, we merge; otherwise ignore. */
        }
      } else {
        /* Direct move insert when key doesn't exist */
        mgp_map_insert_move(target, key_str, src_val);
      }
    } while (mgp_map_items_iterator_next(iter, &item) == MGP_ERROR_NO_ERROR && item);
  }
  mgp_map_items_iterator_destroy(iter);
}

// Helper function to create a complete node map with properties and labels
static struct mgp_map *create_complete_node_map(struct mgp_vertex *node, const filter_config_t *filter_config,
                                                struct mgp_memory *memory) {
  if (!node || !memory) {
    return NULL;
  }

  struct mgp_map *node_map = NULL;
  if (mgp_unordered_map_make_empty(memory, &node_map) != MGP_ERROR_NO_ERROR || node_map == NULL) {
    return NULL;
  }

  // Add node properties with filtering
  struct mgp_properties_iterator *iter = NULL;
  enum mgp_error error = mgp_vertex_iter_properties(node, memory, &iter);
  if (error == MGP_ERROR_NO_ERROR && iter) {
    struct mgp_property *prop = NULL;
    if (mgp_properties_iterator_get(iter, &prop) == MGP_ERROR_NO_ERROR && prop != NULL) {
      do {
        // Check if property should be included based on filtering
        if (should_include_node_property(prop->name, filter_config, node)) {
          mgp_map_insert_move(node_map, prop->name, prop->value);  // inputs are copied, so should be okay to move
        }
      } while (mgp_properties_iterator_next(iter, &prop) == MGP_ERROR_NO_ERROR && prop != NULL);
    }
    mgp_properties_iterator_destroy(iter);
  }

  // Add node labels as _type
  size_t label_count = 0;
  if (mgp_vertex_labels_count(node, &label_count) == MGP_ERROR_NO_ERROR && label_count > 0) {
    if (label_count == 1) {
      // Single label
      struct mgp_label label = {.name = NULL};
      if (mgp_vertex_label_at(node, 0, &label) == MGP_ERROR_NO_ERROR && label.name) {
        struct mgp_value *type_value = NULL;
        if (mgp_value_make_string(label.name, memory, &type_value) == MGP_ERROR_NO_ERROR && type_value != NULL) {
          mgp_map_insert_move(node_map, "_type", type_value);
        }
      }
    } else {
      // Multiple labels - create a list
      struct mgp_list *label_list = NULL;
      if (mgp_list_make_empty(label_count, memory, &label_list) == MGP_ERROR_NO_ERROR && label_list != NULL) {
        for (size_t i = 0; i < label_count; i++) {
          struct mgp_label label = {.name = NULL};
          if (mgp_vertex_label_at(node, i, &label) == MGP_ERROR_NO_ERROR && label.name) {
            struct mgp_value *label_value = NULL;
            if (mgp_value_make_string(label.name, memory, &label_value) == MGP_ERROR_NO_ERROR && label_value != NULL) {
              mgp_list_append_move(label_list, label_value);
            }
          }
        }
        struct mgp_value *type_value = NULL;
        if (mgp_value_make_list(label_list, &type_value) == MGP_ERROR_NO_ERROR && type_value != NULL) {
          mgp_map_insert_move(node_map, "_type", type_value);
        }
      }
    }
  }

  // Add node ID
  struct mgp_vertex_id id;
  if (mgp_vertex_get_id(node, &id) == MGP_ERROR_NO_ERROR) {
    struct mgp_value *id_value = NULL;
    if (mgp_value_make_int(id.as_int, memory, &id_value) == MGP_ERROR_NO_ERROR && id_value != NULL) {
      mgp_map_insert_move(node_map, "_id", id_value);
    }
  }

  return node_map;
}

// Helper function to build tree from a single path recursively
static struct mgp_value *build_tree_from_path_recursive(struct mgp_path *path, size_t start_index, bool lower_case_rels,
                                                        const filter_config_t *filter_config,
                                                        struct mgp_memory *memory) {
  if (!path || !memory) {
    return NULL;
  }

  // Get current node
  struct mgp_vertex *current_node = NULL;
  if (mgp_path_vertex_at(path, start_index, &current_node) != MGP_ERROR_NO_ERROR || current_node == NULL) {
    return NULL;
  }

  // Create node map with properties and labels
  struct mgp_map *node_map = create_complete_node_map(current_node, filter_config, memory);
  if (!node_map) {
    return NULL;
  }

  size_t path_size = 0;
  if (mgp_path_size(path, &path_size) != MGP_ERROR_NO_ERROR) {
    mgp_map_destroy(node_map);
    return NULL;
  }

  // If this is the last node in the path, return just the node
  if (start_index >= path_size) {
    struct mgp_value *result = NULL;
    if (mgp_value_make_map(node_map, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
      mgp_map_destroy(node_map);
      return NULL;
    }
    return result;
  }

  // Get the relationship
  struct mgp_edge *edge = NULL;
  if (mgp_path_edge_at(path, start_index, &edge) != MGP_ERROR_NO_ERROR || edge == NULL) {
    mgp_map_destroy(node_map);
    return NULL;
  }

  // Get relationship type
  struct mgp_edge_type edge_type = {.name = NULL};
  if (mgp_edge_get_type(edge, &edge_type) != MGP_ERROR_NO_ERROR || edge_type.name == NULL) {
    mgp_map_destroy(node_map);
    return NULL;
  }

  const char *rel_type_str = edge_type.name;
  if (!rel_type_str) {
    mgp_map_destroy(node_map);
    return NULL;
  }

  // Process relationship type (lowercase if requested)
  char *rel_type_processed = (char *)rel_type_str;
  if (lower_case_rels) {
    rel_type_processed = to_lowercase(rel_type_str, memory);
    if (!rel_type_processed) {
      mgp_map_destroy(node_map);
      return NULL;
    }
  }

  // Recursively build the subtree for the next part of the path
  struct mgp_value *subtree_value =
      build_tree_from_path_recursive(path, start_index + 1, lower_case_rels, filter_config, memory);
  if (!subtree_value) {
    if (lower_case_rels && rel_type_processed != rel_type_str) {
      mgp_free(memory, rel_type_processed);
    }
    mgp_map_destroy(node_map);
    return NULL;
  }

  // Add relationship ID to the subtree
  struct mgp_edge_id edge_id;
  if (mgp_edge_get_id(edge, &edge_id) == MGP_ERROR_NO_ERROR) {
    char *id_key = concatenate_strings(rel_type_processed, "._id", memory);
    if (id_key) {
      struct mgp_value *id_value = NULL;
      if (mgp_value_make_int(edge_id.as_int, memory, &id_value) == MGP_ERROR_NO_ERROR && id_value != NULL) {
        // Get the subtree map to add the ID
        struct mgp_map *subtree_map = NULL;
        if (mgp_value_get_map(subtree_value, &subtree_map) == MGP_ERROR_NO_ERROR && subtree_map != NULL) {
          mgp_map_insert_move(subtree_map, id_key, id_value);
          // mgp_value_destroy(id_value);
        }
      }
      mgp_free(memory, id_key);
    }
  }

  // Add relationship properties to the subtree with filtering
  struct mgp_properties_iterator *iter = NULL;
  enum mgp_error error = mgp_edge_iter_properties(edge, memory, &iter);
  if (error == MGP_ERROR_NO_ERROR && iter) {
    struct mgp_property *prop = NULL;
    if (mgp_properties_iterator_get(iter, &prop) == MGP_ERROR_NO_ERROR && prop) {
      do {
        if (should_include_rel_property(prop->name, filter_config, rel_type_processed)) {
          /* Build key "<rel>.<prop>" without heap allocs */
          char keybuf[(MAX_STRING_LENGTH * 2) + 2];
          int nw = snprintf(keybuf, sizeof(keybuf), "%s.%s", rel_type_processed, prop->name);
          if (nw > 0 && (size_t)nw < sizeof(keybuf)) {
            struct mgp_map *subtree_map = NULL;
            if (mgp_value_get_map(subtree_value, &subtree_map) == MGP_ERROR_NO_ERROR && subtree_map) {
              mgp_map_insert_move(subtree_map, keybuf, prop->value);
            }
          }
        }
      } while (mgp_properties_iterator_next(iter, &prop) == MGP_ERROR_NO_ERROR && prop);
    }
    mgp_properties_iterator_destroy(iter);
  }

  // Create list for this relationship type
  struct mgp_list *rel_list = NULL;
  if (mgp_list_make_empty(1, memory, &rel_list) != MGP_ERROR_NO_ERROR || rel_list == NULL) {
    if (lower_case_rels && rel_type_processed != rel_type_str) {
      mgp_free(memory, rel_type_processed);
    }
    mgp_value_destroy(subtree_value);
    mgp_map_destroy(node_map);
    return NULL;
  }

  // Add the subtree to the relationship list
  if (mgp_list_append_move(rel_list, subtree_value) != MGP_ERROR_NO_ERROR) {
    if (lower_case_rels && rel_type_processed != rel_type_str) {
      mgp_free(memory, rel_type_processed);
    }
    mgp_list_destroy(rel_list);
    mgp_value_destroy(subtree_value);
    mgp_map_destroy(node_map);
    return NULL;
  }
  // Don't destroy subtree_value - it was moved

  // Add relationship list to node map
  struct mgp_value *rel_list_value = NULL;
  if (mgp_value_make_list(rel_list, &rel_list_value) != MGP_ERROR_NO_ERROR || rel_list_value == NULL) {
    if (lower_case_rels && rel_type_processed != rel_type_str) {
      mgp_free(memory, rel_type_processed);
    }
    mgp_list_destroy(rel_list);
    mgp_map_destroy(node_map);
    return NULL;
  }

  mgp_map_insert_move(node_map, rel_type_processed, rel_list_value);
  // Don't destroy rel_list_value - it was moved

  if (lower_case_rels && rel_type_processed != rel_type_str) {
    mgp_free(memory, rel_type_processed);
  }

  // Convert node map to value
  struct mgp_value *result = NULL;
  if (mgp_value_make_map(node_map, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
    mgp_map_destroy(node_map);
    return NULL;
  }

  return result;
}

// Helper function to build tree from a single path (wrapper for backward compatibility)
static struct mgp_value *build_simple_tree_from_path(struct mgp_path *path, bool lower_case_rels,
                                                     const filter_config_t *filter_config, struct mgp_memory *memory) {
  return build_tree_from_path_recursive(path, 0, lower_case_rels, filter_config, memory);
}

// Main tree conversion function
static struct mgp_value *convert_to_tree_impl(struct mgp_value *value, bool lower_case_rels, struct mgp_map *config,
                                              struct mgp_memory *memory) {
  // Parse filter config once at the start
  filter_config_t filter_config;
  parse_filter_config(&filter_config, config, memory);
  if (!value) {
    // Null input: return empty map
    struct mgp_map *empty_map = NULL;
    if (mgp_unordered_map_make_empty(memory, &empty_map) != MGP_ERROR_NO_ERROR || empty_map == NULL) {
      return NULL;
    }
    struct mgp_value *result = NULL;
    if (mgp_value_make_map(empty_map, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
      mgp_map_destroy(empty_map);
      return NULL;
    }
    return result;
  }

  enum mgp_value_type value_type = MGP_VALUE_TYPE_NULL;
  if (mgp_value_get_type(value, &value_type) != MGP_ERROR_NO_ERROR) {
    return NULL;
  }

  if (value_type == MGP_VALUE_TYPE_NULL) {
    // Null input: return empty map
    struct mgp_map *empty_map = NULL;
    if (mgp_unordered_map_make_empty(memory, &empty_map) != MGP_ERROR_NO_ERROR || empty_map == NULL) {
      return NULL;
    }
    struct mgp_value *result = NULL;
    if (mgp_value_make_map(empty_map, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
      mgp_map_destroy(empty_map);
      return NULL;
    }
    return result;
  }

  // Handle different input types
  if (value_type == MGP_VALUE_TYPE_PATH) {
    // Convert single path to tree
    struct mgp_path *path = NULL;
    if (mgp_value_get_path(value, &path) != MGP_ERROR_NO_ERROR || path == NULL) {
      return NULL;
    }
    return build_simple_tree_from_path(path, lower_case_rels, &filter_config, memory);
  }

  if (value_type == MGP_VALUE_TYPE_LIST) {
    // Convert list of paths to tree
    struct mgp_list *paths = NULL;
    if (mgp_value_get_list(value, &paths) != MGP_ERROR_NO_ERROR || paths == NULL) {
      return NULL;
    }

    size_t paths_size = 0;
    if (mgp_list_size(paths, &paths_size) != MGP_ERROR_NO_ERROR) {
      return NULL;
    }

    if (paths_size == 0) {
      // Empty list: return empty map
      struct mgp_map *empty_map = NULL;
      if (mgp_unordered_map_make_empty(memory, &empty_map) != MGP_ERROR_NO_ERROR || empty_map == NULL) {
        return NULL;
      }
      struct mgp_value *result = NULL;
      if (mgp_value_make_map(empty_map, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
        mgp_map_destroy(empty_map);
        return NULL;
      }
      return result;
    }

    // Group paths by root node ID to build separate trees
    struct mgp_map *root_groups[MAX_PROPERTIES];  // Map from root_id to tree
    size_t root_group_count = 0;
    int64_t root_ids[MAX_PROPERTIES];

    for (size_t i = 0; i < paths_size; i++) {
      struct mgp_value *path_value = NULL;
      if (mgp_list_at(paths, i, &path_value) != MGP_ERROR_NO_ERROR || path_value == NULL) {
        continue;
      }

      struct mgp_path *path = NULL;
      if (mgp_value_get_path(path_value, &path) != MGP_ERROR_NO_ERROR || path == NULL) {
        continue;
      }

      // Get root node ID
      struct mgp_vertex *root_node = NULL;
      if (mgp_path_vertex_at(path, 0, &root_node) != MGP_ERROR_NO_ERROR || root_node == NULL) {
        continue;
      }

      struct mgp_vertex_id root_id;
      if (mgp_vertex_get_id(root_node, &root_id) != MGP_ERROR_NO_ERROR) {
        continue;
      }

      // Find existing root group or create new one
      size_t group_index = SIZE_MAX;
      for (size_t j = 0; j < root_group_count && j < MAX_PROPERTIES; j++) {
        if (root_ids[j] == root_id.as_int) {
          group_index = j;
          break;
        }
      }

      if (group_index == SIZE_MAX) {
        // New root group
        if (root_group_count >= MAX_PROPERTIES) {
          continue;  // Too many root groups
        }
        group_index = root_group_count;
        root_ids[group_index] = root_id.as_int;
        root_groups[group_index] = NULL;
        root_group_count++;
      }

      // Convert this path to a tree
      struct mgp_value *tree_value = build_simple_tree_from_path(path, lower_case_rels, &filter_config, memory);
      if (!tree_value) {
        continue;
      }

      // Get the tree map
      struct mgp_map *tree_map = NULL;
      if (mgp_value_get_map(tree_value, &tree_map) != MGP_ERROR_NO_ERROR || tree_map == NULL) {
        mgp_value_destroy(tree_value);
        continue;
      }

      if (root_groups[group_index] == NULL) {
        // First tree for this root, use it as the base
        root_groups[group_index] = tree_map;
        // Don't destroy tree_value here since we're keeping the map
      } else {
        // Merge with existing tree for this root
        merge_trees(root_groups[group_index], tree_map, memory);
        mgp_value_destroy(tree_value);
      }
    }

    if (root_group_count == 0) {
      // No valid paths found, return empty map
      struct mgp_map *empty_map = NULL;
      if (mgp_unordered_map_make_empty(memory, &empty_map) != MGP_ERROR_NO_ERROR || empty_map == NULL) {
        return NULL;
      }
      struct mgp_value *result = NULL;
      if (mgp_value_make_map(empty_map, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
        mgp_map_destroy(empty_map);
        return NULL;
      }
      return result;
    }

    if (root_group_count == 1) {
      if (root_groups[0] == NULL) {
        return NULL;
      }
      // Single root: return the tree directly
      struct mgp_value *result = NULL;
      if (mgp_value_make_map(root_groups[0], &result) != MGP_ERROR_NO_ERROR || result == NULL) {
        mgp_map_destroy(root_groups[0]);
        return NULL;
      }
      return result;
    }

    // Multiple roots: return a list of all root trees
    struct mgp_list *tree_list = NULL;
    if (mgp_list_make_empty(root_group_count, memory, &tree_list) != MGP_ERROR_NO_ERROR || tree_list == NULL) {
      // Clean up on error
      for (size_t i = 0; i < root_group_count && i < MAX_PROPERTIES; i++) {
        if (root_groups[i]) {
          mgp_map_destroy(root_groups[i]);
        }
      }
      return NULL;
    }

    for (size_t i = 0; i < root_group_count && i < MAX_PROPERTIES; i++) {
      if (root_groups[i]) {
        struct mgp_value *tree_value = NULL;
        if (mgp_value_make_map(root_groups[i], &tree_value) != MGP_ERROR_NO_ERROR || tree_value == NULL) {
          mgp_list_destroy(tree_list);
          // Clean up remaining trees
          for (size_t j = i; j < root_group_count && j < MAX_PROPERTIES; j++) {
            if (root_groups[j]) {
              mgp_map_destroy(root_groups[j]);
            }
          }
          return NULL;
        }
        mgp_list_append_move(tree_list, tree_value);
        // Don't destroy tree_value - it was moved
      }
    }

    struct mgp_value *result = NULL;
    if (mgp_value_make_list(tree_list, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
      mgp_list_destroy(tree_list);
      return NULL;
    }
    return result;
  }

  // For other types, return as-is
  struct mgp_value *result = NULL;
  if (mgp_value_copy(value, memory, &result) != MGP_ERROR_NO_ERROR || result == NULL) {
    return NULL;
  }
  return result;
}

// Main to_tree procedure implementation
static void to_tree(struct mgp_list *args, struct mgp_graph *graph, struct mgp_result *result,
                    struct mgp_memory *memory) {
  size_t args_size = 0;
  if (mgp_list_size(args, &args_size) != MGP_ERROR_NO_ERROR) {
    mgp_result_set_error_msg(result, "Failed to get arguments size");
    return;
  }

  if (args_size == 0) {
    mgp_result_set_error_msg(result, "to_tree requires at least one argument");
    return;
  }

  // Get the input argument
  struct mgp_value *input = NULL;
  if (mgp_list_at(args, 0, &input) != MGP_ERROR_NO_ERROR || input == NULL) {
    mgp_result_set_error_msg(result, "Failed to get input argument");
    return;
  }

  // Get lowerCaseRels parameter (default to true)
  bool lower_case_rels = true;
  if (args_size > 1) {
    struct mgp_value *lower_case_arg = NULL;
    if (mgp_list_at(args, 1, &lower_case_arg) == MGP_ERROR_NO_ERROR && lower_case_arg != NULL) {
      enum mgp_value_type arg_type = MGP_VALUE_TYPE_NULL;
      if (mgp_value_get_type(lower_case_arg, &arg_type) == MGP_ERROR_NO_ERROR && arg_type == MGP_VALUE_TYPE_BOOL) {
        int bool_val = 0;
        if (mgp_value_get_bool(lower_case_arg, &bool_val) == MGP_ERROR_NO_ERROR) {
          lower_case_rels = (bool_val != 0);
        }
      }
    }
  }

  // Get config parameter (default to empty map)
  struct mgp_map *config = NULL;
  if (args_size > 2) {
    struct mgp_value *config_arg = NULL;
    if (mgp_list_at(args, 2, &config_arg) == MGP_ERROR_NO_ERROR && config_arg != NULL) {
      enum mgp_value_type arg_type = MGP_VALUE_TYPE_NULL;
      if (mgp_value_get_type(config_arg, &arg_type) == MGP_ERROR_NO_ERROR && arg_type == MGP_VALUE_TYPE_MAP) {
        if (mgp_value_get_map(config_arg, &config) != MGP_ERROR_NO_ERROR || config == NULL) {
          mgp_result_set_error_msg(result, "Failed to get config map");
          return;
        }
        size_t size = 0;
        if (mgp_map_size(config, &size) == MGP_ERROR_NO_ERROR && size == 0) {
          // Config map is empty, set to NULL
          config = NULL;
        }
      }
    }
  }

  // Convert the input value to tree structure
  struct mgp_value *result_value = convert_to_tree_impl(input, lower_case_rels, config, memory);
  if (!result_value) {
    mgp_result_set_error_msg(result, "Failed to convert input to tree");
    return;
  }

  // Handle multiple results (APOC behavior for multiple roots)
  enum mgp_value_type result_type = MGP_VALUE_TYPE_NULL;
  if (mgp_value_get_type(result_value, &result_type) != MGP_ERROR_NO_ERROR) {
    mgp_value_destroy(result_value);
    mgp_result_set_error_msg(result, "Failed to get result type");
    return;
  }

  if (result_type == MGP_VALUE_TYPE_LIST) {
    // Multiple roots: yield one record per root
    struct mgp_list *result_list = NULL;
    if (mgp_value_get_list(result_value, &result_list) != MGP_ERROR_NO_ERROR || result_list == NULL) {
      mgp_value_destroy(result_value);
      mgp_result_set_error_msg(result, "Failed to get result list");
      return;
    }

    size_t list_size = 0;
    if (mgp_list_size(result_list, &list_size) != MGP_ERROR_NO_ERROR) {
      mgp_value_destroy(result_value);
      mgp_result_set_error_msg(result, "Failed to get list size");
      return;
    }

    for (size_t i = 0; i < list_size; i++) {
      struct mgp_value *tree_value = NULL;
      if (mgp_list_at(result_list, i, &tree_value) != MGP_ERROR_NO_ERROR || tree_value == NULL) {
        continue;
      }

      struct mgp_result_record *record = NULL;
      if (mgp_result_new_record(result, &record) != MGP_ERROR_NO_ERROR || record == NULL) {
        mgp_value_destroy(result_value);
        mgp_result_set_error_msg(result, "Failed to create result record");
        return;
      }

      if (mgp_result_record_insert(record, kReturnValue, tree_value) != MGP_ERROR_NO_ERROR) {
        mgp_value_destroy(result_value);
        mgp_result_set_error_msg(result, "Failed to insert result value");
        return;
      }
    }
  } else {
    // Single root: yield one record
    struct mgp_result_record *record = NULL;
    if (mgp_result_new_record(result, &record) != MGP_ERROR_NO_ERROR || record == NULL) {
      mgp_value_destroy(result_value);
      mgp_result_set_error_msg(result, "Failed to create result record");
      return;
    }

    if (mgp_result_record_insert(record, kReturnValue, result_value) != MGP_ERROR_NO_ERROR) {
      mgp_value_destroy(result_value);
      mgp_result_set_error_msg(result, "Failed to insert result value");
      return;
    }
  }
}

// Module initialization
int mgp_init_module(struct mgp_module *module, struct mgp_memory *memory) {
  // Add the to_tree procedure
  struct mgp_proc *proc = NULL;
  if (mgp_module_add_read_procedure(module, kProcedureToTree, to_tree, &proc) != MGP_ERROR_NO_ERROR || proc == NULL) {
    return 1;
  }

  // Add parameters
  struct mgp_type *any_type = NULL;
  if (mgp_type_any(&any_type) != MGP_ERROR_NO_ERROR || any_type == NULL) {
    return 1;
  }

  struct mgp_type *nullable_any_type = NULL;
  if (mgp_type_nullable(any_type, &nullable_any_type) != MGP_ERROR_NO_ERROR || nullable_any_type == NULL) {
    return 1;
  }

  if (mgp_proc_add_arg(proc, kParameterPaths, nullable_any_type) != MGP_ERROR_NO_ERROR) {
    return 1;
  }

  struct mgp_type *bool_type = NULL;
  if (mgp_type_bool(&bool_type) != MGP_ERROR_NO_ERROR || bool_type == NULL) {
    return 1;
  }

  struct mgp_value *true_value = NULL;
  if (mgp_value_make_bool(1, memory, &true_value) != MGP_ERROR_NO_ERROR || true_value == NULL) {
    return 1;
  }

  if (mgp_proc_add_opt_arg(proc, kParameterLowerCaseRels, bool_type, true_value) != MGP_ERROR_NO_ERROR) {
    mgp_value_destroy(true_value);
    return 1;
  }
  mgp_value_destroy(true_value);

  struct mgp_type *map_type = NULL;
  if (mgp_type_map(&map_type) != MGP_ERROR_NO_ERROR || map_type == NULL) {
    return 1;
  }

  struct mgp_map *empty_map = NULL;
  if (mgp_unordered_map_make_empty(memory, &empty_map) != MGP_ERROR_NO_ERROR || empty_map == NULL) {
    return 1;
  }

  struct mgp_value *empty_map_value = NULL;
  if (mgp_value_make_map(empty_map, &empty_map_value) != MGP_ERROR_NO_ERROR || empty_map_value == NULL) {
    mgp_map_destroy(empty_map);
    return 1;
  }

  if (mgp_proc_add_opt_arg(proc, kParameterConfig, map_type, empty_map_value) != MGP_ERROR_NO_ERROR ||
      empty_map_value == NULL) {
    mgp_value_destroy(empty_map_value);
    return 1;
  }
  mgp_value_destroy(empty_map_value);

  // Add return value
  if (mgp_proc_add_result(proc, kReturnValue, map_type) != MGP_ERROR_NO_ERROR || map_type == NULL) {
    return 1;
  }

  return 0;
}

int mgp_shutdown_module() { return 0; }
