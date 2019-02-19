#include <benchmark/benchmark.h>
#include <benchmark/benchmark_api.h>
#include <glog/logging.h>

#include <random>

#include "database/single_node/graph_db.hpp"
#include "database/single_node/graph_db_accessor.hpp"

const constexpr size_t kNumOfConstraints = 10;
const constexpr size_t kNumOfPropsInConstraint = 5;

const constexpr size_t kNumOfProps = 100;
const constexpr size_t kNumOfLabels = 10;

const constexpr size_t kNumOfVertices = 10000;

struct TestSet {
 public:
  TestSet() {
    constraint_labels.reserve(kNumOfConstraints);
    for (size_t i = 0; i < kNumOfConstraints; ++i) {
      constraint_labels.emplace_back("clabel_" + std::to_string(i));
    }

    constraint_props.reserve(kNumOfConstraints * kNumOfPropsInConstraint);
    for (size_t i = 0; i < kNumOfConstraints * kNumOfPropsInConstraint; ++i) {
      constraint_props.emplace_back("cprop_" + std::to_string(i));
    }

    labels.reserve(kNumOfLabels);
    for (size_t i = 0; i < kNumOfLabels; ++i) {
      labels.emplace_back("label_" + std::to_string(i));
    }

    props.reserve(kNumOfProps);
    for (size_t i = 0; i < kNumOfProps; ++i) {
      props.emplace_back("prop_" + std::to_string(i));
    }
  }

  std::vector<std::string> constraint_labels;
  std::vector<std::string> constraint_props;

  std::vector<std::string> labels;
  std::vector<std::string> props;

  long short_prop_value{0};
  std::string long_prop_value{20, ' '};
};

void Run(benchmark::State &state, bool enable_constraint) {
  while (state.KeepRunning()) {
    database::GraphDb db;
    TestSet test_set;

    // CreateExistence constraints
    if (enable_constraint) {
      state.PauseTiming();
      for (size_t i = 0; i < kNumOfConstraints; ++i) {
        auto dba = db.Access();
        auto label = dba->Label(test_set.constraint_labels.at(i));
        std::vector<storage::Property> props;
        props.reserve(kNumOfPropsInConstraint);
        for (size_t j = 0; j < kNumOfPropsInConstraint; ++j) {
          props.push_back(dba->Property(
              test_set.constraint_props.at(kNumOfPropsInConstraint * i + j)));
        }
        dba->BuildExistenceConstraint(label, props);
        dba->Commit();
      }
      state.ResumeTiming();
    }

    // Add all verices and also add labels and properties
    std::vector<gid::Gid> vertices;
    vertices.reserve(kNumOfVertices);
    for (size_t k = 0; k < kNumOfVertices; ++k) {
      auto dba = db.Access();
      auto v = dba->InsertVertex();
      vertices.push_back(v.gid());

      // Labels and properties that define constraints
      for (size_t i = 0; i < kNumOfConstraints; ++i) {
        for (size_t j = 0; j < kNumOfPropsInConstraint; ++j) {
          v.PropsSet(dba->Property(test_set.constraint_props.at(
                         kNumOfPropsInConstraint * i + j)),
                     test_set.short_prop_value);
        }
        auto label = dba->Label(test_set.constraint_labels.at(i));
        v.add_label(label);
      }

      // Add other labels
      for (auto label : test_set.labels) {
        v.add_label(dba->Label(label));
      }

      // Add other properties
      for (auto prop : test_set.props) {
        v.PropsSet(dba->Property(prop), test_set.short_prop_value);
      }

      dba->Commit();
    }

    // Delete all properties and labels
    for (auto gid : vertices) {
      auto dba = db.Access();
      auto v = dba->FindVertex(gid, false);
      std::vector<storage::Label> labels_to_del(v.labels());
      for (auto &label : labels_to_del) {
        v.remove_label(label);
      }

      v.PropsClear();
    }

    // Delete all vertices
    for (auto gid : vertices) {
      auto dba = db.Access();
      auto v = dba->FindVertex(gid, false);
      dba->RemoveVertex(v);
      dba->Commit();
    }
  }
}

BENCHMARK_CAPTURE(Run, WithoutConstraint, false)->Unit(benchmark::kMillisecond);

BENCHMARK_CAPTURE(Run, WithConstraint, true)->Unit(benchmark::kMillisecond);

BENCHMARK_MAIN();
