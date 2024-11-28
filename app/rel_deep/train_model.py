import os
import torch

import relbench
import numpy as np
import os
import math
import numpy as np
from tqdm import tqdm

import torch
import torch_geometric
import torch_frame

from torch.nn import BCEWithLogitsLoss, L1Loss
from relbench.datasets import get_dataset
from relbench.tasks import get_task

dataset = get_dataset("rel-f1", download=True)
task = get_task("rel-f1", "driver-position", download=True)

train_table = task.get_table("train")
val_table = task.get_table("val")
test_table = task.get_table("test")

out_channels = 1
loss_fn = L1Loss()
tune_metric = "mae"
higher_is_better = False


# Some book keeping
from torch_geometric.seed import seed_everything

seed_everything(42)


device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
print(device)  # check that it's cuda if you want it to run in reasonable time!
root_dir = "./data"