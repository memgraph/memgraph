
/**
 * Creates a vector of records accessors (Edge or Vertex).
 *
 * @tparam TAccessor The type of accessor to create a vector of.
 * @tparam TCollection An iterable of pointers to version list objects.
 *
 * @param records An iterable of version list pointers for which accessors
 *  need to be created.
 * @param db_accessor A database accessor to create the record accessors with.
 */
template <typename TAccessor, typename TCollection>
std::vector<TAccessor> make_accessors(
    const TCollection &records,
    GraphDbAccessor &db_accessor) {

  std::vector<TAccessor> accessors;
  accessors.reserve(records.size());

  for (auto record : records)
    accessors.emplace_back(*record, db_accessor);

  return accessors;
}
