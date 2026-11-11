import torch
import datetime
import torch.nn as nn
import json
from ax.service.ax_client import AxClient, ObjectiveProperties
from helper import generate_latency_map_intersect, get_data_loaders, get_relative_path, make_dataloader
from train import train, evaluate
from ax.utils.notebook.plotting import render
from OurModels.EncoderDecoder.bvae import BVAE
from OurModels.EncoderDecoder.model import VAE
from OurModels.EncoderDecoder.betaCVAE.model import BetaCVAE
from OurModels.EncoderDecoder.carbVAE.model import CarbVAE


def do_hyperparameter_BO(
    model_class: nn.Module,
    in_dim:int,
    out_dim:int,
    loss_function:nn.Module,
    device: torch.device,
    parameters_path: str,
    lr,
    epochs,
    trials,
    plots,
    train_data,
    test_data = None,
    val_data = None,
    weights:dict=None,
    best_parameters=None,
    mean=0,
    std=0
    ):

    is_retraining = best_parameters is not None
    ax_client = AxClient(random_seed=42)
    parameters = [
        {
            'name': 'lr',
            'type': 'range',
            'bounds': lr,
            'value_type': 'float',
            "log_scale": True,
        },
        #{
        #    'name': 'dropout',
        #    'type': 'range',
        #    'bounds': [0.0, 0.4],hyper
        #    'value_type': 'float'
        #},
        {
            'name': 'gradient_norm',
            'type': 'range',
            'bounds': [0.5, 2.5],
            'value_type': 'float'
        },
        {
            'name': 'batch_size',
            'type': 'range',
            'bounds': [32, 64],
            'value_type': 'int'
        },
        {
            'name': 'patience',
            'type': 'range',
            'bounds': [25, 50],
            'value_type': 'int'
        },
    ]


    if model_class == BVAE or model_class == BetaCVAE:
        parameters.append({
            'name': 'beta',
            'type': 'range',
            'bounds': [0.1, 5],
            'value_type': 'float',
            "log_scale": False,
        })

        parameters.append({
            'name': 'z_dim',
            'type': 'range',
            'bounds': [1, 128],
            'value_type': 'int',
            'is_ordered': True,
            'sort_values' : True
        })

        parameters.append({
            'name': 'weight_decay',
            'type': 'range',
            'bounds': [0, 0.1],
            'value_type': 'float',
            "log_scale": False
        })

    if model_class == CarbVAE:
        """
        parameters.append({
            'name': 'beta',
            'type': 'range',
            'bounds': [1, 3],
            'value_type': 'float',
            "log_scale": True,
        })
        """

        parameters.append({
            'name': 'z_dim',
            'type': 'choice',
            #'bounds': [8, 64],
            'values': [8, 16, 24, 32],
            'value_type': 'int',
            'is_ordered': True,
            'sort_values' : True,
        })

        parameters.append({
            'name': 'weight_decay',
            'type': 'range',
            'bounds': [0, 0.01],
            'value_type': 'float',
            "log_scale": False
        })

        """
        parameters.append({
            'name': 'gamma',
            'type': 'range',
            'bounds': [4, 10],
            'value_type': 'float',
            "log_scale": True,
        })

        parameters.append({
            'name': 'delta',
            'type': 'range',
            'bounds': [2, 6],
            'value_type': 'float',
            "log_scale": True,
        })
        """

    torch.manual_seed(42)

    if best_parameters is None:
        ax_client.create_experiment(
            name='tune_model',
            parameters=parameters,
            objectives={
                'loss': ObjectiveProperties(minimize=True),
            },
        )

        trial_eval_map = list()

        for _ in range(trials):
            parameters, trial_index = ax_client.get_next_trial()
            raw_data = train_evaluate(
                params=parameters,
                loss_function=loss_function,
                model_class=model_class,
                train_data=train_data,
                val_data=val_data,
                test_data=test_data,
                in_dim=in_dim,
                out_dim=out_dim,
                epochs=epochs,
                weights=weights,
            )
            print(f"Parameters: {parameters}")
            print(f"raw_data: {raw_data}")
            trial_eval_map.append((parameters, raw_data))
            ax_client.complete_trial(trial_index=trial_index, raw_data=raw_data)

        best_parameters, _ = ax_client.get_best_parameters()
        print(trial_eval_map)
        print(f"Loss of best_parameters {list(filter(lambda x: x[0] == best_parameters, trial_eval_map))}")

    if best_parameters is not None and (model_class == BVAE or model_class == VAE or model_class == BetaCVAE or model_class == CarbVAE):
        batch_size = best_parameters.get('batch_size')
        samples_needed = batch_size-(batch_size%len(train_data))
        print("Starting batch generation ", batch_size, " lenght of data: ", len(train_data))
        print("with samples needed: ", samples_needed)

        if samples_needed > 0:
            for i in range(samples_needed+batch_size*10):
                elem = train_data.__getitem__(i%len(train_data))
                train_data.append(elem)

    train_data_loader, val_data_loader, test_data_loader = get_data_loaders(
        data=train_data,
        test_data=test_data,
        val_data=val_data,
        batch_size=best_parameters.get('batch_size', 1),
    )

    print(f'\nBest model training with parameters: {best_parameters}', flush=True)

    if model_class == BVAE or model_class == BetaCVAE:
        l_function = loss_function(
            beta=best_parameters.get('beta', 1.0),
        )
    elif model_class == CarbVAE:
        l_function = loss_function(
            beta=parameters.get('beta', 1.5),
            gamma=parameters.get('gamma', 4.0),
            delta=parameters.get('delta', 2.0)
        )
    else:
        l_function = loss_function()


    best_model, tree, target = train(
        model_class=model_class,
        train_data_loader=train_data_loader,
        test_data_loader=test_data_loader,
        val_data_loader=val_data_loader,
        in_dim=in_dim,
        out_dim=out_dim,
        loss_function=l_function,
        device=device,
        parameters=best_parameters,
        epochs=epochs,
        weights=weights
    )
    test_accuracy = evaluate(best_model, val_data_loader=val_data_loader, loss_function=l_function, device=device, batch_size=best_parameters.get("batch_size", 1))

    # write best parameters to file
    if not is_retraining:
        with open(parameters_path, 'w') as file:
            if model_class == CarbVAE:
                best_parameters["mean"] = mean
                best_parameters["std"] = std

            json.dump(best_parameters, file)

    if plots:
        try:
            ax_path = get_relative_path(f"{type(best_model).__name__}.json", 'Logs')  #get_relative_path(file_name=f'{type(best_model).__name__}{datetime.datetime.now().strftime("%d-%H:%M:%S")}.json', dir='Logs')
            ax_client.save_to_json_file(ax_path)
            render(ax_client.get_optimization_trace())
            render(ax_client.get_contour_plot(param_x="lr", param_y="batch_size", metric_name="loss"))
        except:
            print('Could not produce plots')

    print(f'Best model loss test set: {test_accuracy}', flush=True)
    return best_model, tree, target

def get_loss_function(model_class: type, parameters: dict, loss_function: nn.Module) -> nn.Module:
    if model_class == BVAE or model_class == BetaCVAE:
         return loss_function(
            beta=parameters.get('beta', 1.0),
        )
    elif model_class == CarbVAE:
        return loss_function(
            beta=parameters.get('beta', 1.5),
            gamma=parameters.get('gamma', 4.0),
            delta=parameters.get('delta', 2.0)
        )

    return loss_function()

def train_evaluate(
    params: dict,
    loss_function: nn.Module,
    model_class: type,
    train_data,
    val_data,
    test_data,
    in_dim: int,
    out_dim: int,
    epochs: int,
    weights: dict,
):
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")
    batch_size = params.get('batch_size', 1)
    #batch_size = 1

    train_data_loader, val_data_loader, test_data_loader = get_data_loaders(
        data=train_data,
        test_data=test_data,
        val_data=val_data,
        batch_size=batch_size
    )

    print(f"Train data: {train_data_loader}")
    print(f"Test data: {test_data_loader}")
    print(f"Val data: {val_data_loader}")

    l_function = get_loss_function(model_class, params, loss_function)

    print(f'Batch size: {batch_size}')
    print(f'Training batches: {len(train_data_loader)} Test batches: {len(test_data_loader)} Validation batches: {len(val_data_loader)} \n', flush=True)

    print(f"[TRAIN_EVALUATE] out_dim: {out_dim}, in_dim: {in_dim}")

    model, _, _ = train(
        model_class=model_class,
        train_data_loader=train_data_loader,
        val_data_loader=val_data_loader,
        test_data_loader=test_data_loader,
        in_dim=in_dim,
        out_dim=out_dim,
        loss_function=l_function,
        device=device,
        parameters=params,
        epochs=epochs,
        weights=weights
    )
    print(f"Training on {device}")
    val_loss = evaluate(
        model=model,
        val_data_loader=val_data_loader,
        loss_function=l_function,
        device=device,
        batch_size=params.get("batch_size", 1)
    )
    print(f'Test loss for the model after training {val_loss["loss"]}', flush=True)
    return val_loss['loss']
