import mgp
import torch
from torch_geometric.loader import HGTLoader


def train_epoch(
    model: mgp.Any,
    opt: mgp.Any,
    data: mgp.Any,
    criterion: mgp.Any,
    batch_size: int,
    observed_attribute: str,
    num_samples: dict,
) -> torch.tensor:
    """In this function, one epoch of training is performed.

    Args:
        model (Any): object for model
        opt (Any): model optimizer
        data (Data): prepared dataset for training
        criterion (Any): criterion for loss calculation
        batch_size (int): batch size for training
        observed_attribute (str): observed attribute for training
        num_samples (dict): The number of nodes to
            sample in each iteration and for each node type.

    Returns:
        torch.tensor: loss calculated when training step is performed
    """

    train_input_nodes = (observed_attribute, data[observed_attribute].train_mask)
    val_input_nodes = (observed_attribute, data[observed_attribute].val_mask)

    train_loader = HGTLoader(
        data=data,
        num_samples=num_samples,
        shuffle=True,
        batch_size=batch_size,
        input_nodes=train_input_nodes,
    )

    val_loader = HGTLoader(
        data=data,
        num_samples=num_samples,
        shuffle=False,
        batch_size=batch_size,
        input_nodes=val_input_nodes,
    )

    def training_loop(loader: HGTLoader, gradient: bool) -> float:
        """Loop for either train or validation, depending on the flag gradient.

        Args:
            loader (HGTLoader): train or validation loader
            gradient (bool): True for train, False for validation

        Returns:
            float: returns loss calculated during training or validation
        """
        ret = 0.0

        # Set the model to train or eval mode depending on the flag gradient.
        model.train() if gradient else model.eval()

        for n, batch in enumerate(loader):
            if gradient:
                opt.zero_grad()  # Clear gradients.

            out = model(batch.x_dict, batch.edge_index_dict)[observed_attribute]  # Perform a single forward pass.
            loss = criterion(out, batch[observed_attribute].y)  # Compute the loss solely based on the training nodes.

            if gradient:
                loss.backward()  # Derive gradients.
                opt.step()  # Update parameters based on gradients.

            ret += loss.item()

        return ret / (n + 1)

    ret = training_loop(train_loader, True)
    ret_val = training_loop(val_loader, False)

    return ret, ret_val
