import torch
import torch.nn as nn
from TreeConvolution.tcnn import (BinaryTreeConv, DynamicPooling,
                                  TreeActivation, TreeLayerNorm)

import torch.nn.functional as F

class CostModel(nn.Module):
    def __init__(self, in_dim, **args) -> None:
        super(CostModel, self).__init__()

        self.tree_conv = nn.Sequential(
            BinaryTreeConv(in_dim, 256),
            TreeLayerNorm(),
            TreeActivation(nn.Mish()),
            BinaryTreeConv(256, 128),
            TreeLayerNorm(),
            TreeActivation(nn.Mish()),
            BinaryTreeConv(128, 64),
            TreeLayerNorm(),
            DynamicPooling(),
            nn.Linear(64, 32),
            nn.Mish(),
            nn.Linear(32, 1)
        )

    def forward(self, trees):
        x = self.tree_conv(trees)
        x = torch.squeeze(x, dim=1)
        return x

    def predict(self, trees):
        return torch.expm1(self.forward(trees))

class CostLoss(torch.nn.Module):
    def __init__(self):
        super(CostLoss, self).__init__()

    def forward(self, pred: torch.tensor, target: torch.tensor):
        return {'loss': F.l1_loss(pred, target.float())}
