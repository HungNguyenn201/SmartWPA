import numpy as np
import pandas as pd
from scipy.optimize import curve_fit
from abc import abstractmethod


from typing import Protocol

class Estimator(Protocol):

    @abstractmethod
    def fit(V: pd.Series, P: pd.Series) -> None:
        raise NotImplementedError
    
    @abstractmethod
    def predict(V: pd.Series) -> pd.Series:
        raise NotImplementedError

class Logistic5PL(Estimator):
    def __init__(self):
        self.params = None

    @staticmethod
    def _five_p_logistic(x, A, B, C, D, E):
        return D + (A - D) / np.power((1 + np.power((x / C), B)), E)

    def fit(self, x_train: pd.Series, y_train: pd.Series) -> None:
        p0 = [
            y_train.min(),
            1.0,
            x_train.median(),
            y_train.max(),
            1.0
        ]

        try:
            params, _ = curve_fit(
                self._five_p_logistic, 
                x_train, 
                y_train, 
                p0=p0, 
                maxfev=10000 
            )
            self.params = params
        except RuntimeError:
            print("Optimization failed to converge.")
            self.params = None

    def predict(self, x_input: pd.Series) -> pd.Series:
        if self.params is None:
            raise ValueError("Model is not fitted yet. Call .fit() first.")
        return self._five_p_logistic(x_input, *self.params)

    def get_params(self):
        if self.params is None: 
            return None
        return {
            'A': self.params[0], 'B': self.params[1], 
            'C': self.params[2], 'D': self.params[3], 
            'E': self.params[4]
        }

def power_est(data: pd.DataFrame):
    estimator = Logistic5PL()
    normals = data[data['status'] == 'NORMAL']
    estimator.fit(normals['WIND_SPEED'], normals['ACTIVE_POWER'])
    
    return estimator.predict(data['WIND_SPEED'])
    