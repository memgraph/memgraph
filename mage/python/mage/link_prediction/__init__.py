from mage.link_prediction.constants import (  # noqa: F401
  Activations,
  Aggregators,
  Context,
  Devices,
  Metrics,
  Models,
  Optimizers,
  Parameters,
  Predictors,
  Reindex,
)
from mage.link_prediction.factory import (  # noqa: F401
  create_activation_function,
  create_model,
  create_optimizer,
  create_predictor,
)
from mage.link_prediction.link_prediction_util import (  # noqa: F401
  add_self_loop,
  classify,
  inner_predict,
  inner_train,
  preprocess,
  proj_0,
)
