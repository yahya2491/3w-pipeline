import time

import sklearn
from sklearn.ensemble import RandomForestClassifier
from sklearn.model_selection import GridSearchCV, StratifiedGroupKFold

from config import SEED
from data import load_gold
from evaluate import evaluate, save_importance

d = load_gold()

# 1. tune on train only, grouped by recording
base = RandomForestClassifier(n_estimators=100, max_samples=0.3, n_jobs=4, random_state=SEED)
grid = {"min_samples_leaf": [5, 20, 50], "max_features": ["sqrt", 0.5]}
cv = StratifiedGroupKFold(n_splits=3, shuffle=True, random_state=SEED)

search = GridSearchCV(base, grid, scoring="f1_macro", cv=cv, n_jobs=1, refit=False, verbose=2)
t0 = time.time()
search.fit(d.X_train, d.y_train, groups=d.groups_train, sample_weight=d.w_train)
tune_s = time.time() - t0
print("best params:", search.best_params_, "CV macro F1:", round(search.best_score_, 3))

# 2. final model on all of train
model = RandomForestClassifier(n_estimators=300, max_samples=0.3, n_jobs=4, random_state=SEED, **search.best_params_)
t0 = time.time()
model.fit(d.X_train, d.y_train, sample_weight=d.w_train)
fit_s = time.time() - t0

# 3. test set, used once
evaluate(
    "random_forest",
    d.y_test,
    model.predict(d.X_test),
    d.meta_test,
    extra={
        "best_params": search.best_params_,
        "cv_macro_f1": search.best_score_,
        "cv_results": {str(p): s for p, s in zip(search.cv_results_["params"], search.cv_results_["mean_test_score"])},
        "tune_seconds": tune_s,
        "fit_seconds": fit_s,
        "sklearn_version": sklearn.__version__,
    },
)
save_importance("random_forest", model, d.X_test, d.y_test)
