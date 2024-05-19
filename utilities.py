import pandas as pd
from scipy import stats
import matplotlib.pyplot as plt
import alphalens.performance as perf
import alphalens.utils as utils
import alphalens.plotting as plotting
from alphalens.tears import GridFigure
import warnings


def match_stock_exchange(symbol: str) -> str:
    # 沪市股票包含上证主板和科创板和B股：沪市主板股票代码是60开头、科创板股票代码是688开头、B股代码900开头。
    # 深市股票包含主板、中小板、创业板和B股：深市主板股票代码是000开头、中小板股票代码002开头、创业板300开头、B股代码200开头
    exchange = ""
    if symbol.startswith("60"):
        exchange = "sh"
    elif symbol.startswith("688"):
        exchange = "sh"
    elif symbol.startswith("900"):
        exchange = "sh"
    elif symbol.startswith("00"):
        exchange = "sz"
    elif symbol.startswith("300"):
        exchange = "sz"
    elif symbol.startswith("200"):
        exchange = "sz"
    return exchange


def summary_ic_data(ic_data) -> pd.DataFrame:
    """
    根据alphalens.performance.factor_information_coefficient函数的输出结果，生成ic统计表格
    """
    ic_summary_table = pd.DataFrame()
    ic_summary_table["IC Mean"] = ic_data.mean()
    ic_summary_table["IC Std."] = ic_data.std()
    ic_summary_table["Information Ratio"] = \
        ic_data.mean() / ic_data.std()
    t_stat, p_value = stats.ttest_1samp(ic_data, 0)
    ic_summary_table["t-stat(IC)"] = t_stat
    ic_summary_table["p-value(IC)"] = p_value
    ic_summary_table["IC Skew"] = stats.skew(ic_data)
    ic_summary_table["IC Kurtosis"] = stats.kurtosis(ic_data)

    return ic_summary_table


@plotting.customize
def plot_return(
    factor_data, long_short=True, group_neutral=False, by_group=False, save_pic=False, save_path=None
):
    """
    源自alphalens的create_returns_tear_sheet；
    仅保留画图功能；
    增加是否保存到本地的参数
    """

    factor_returns = perf.factor_returns(
        factor_data, long_short, group_neutral
    )

    mean_quant_ret, std_quantile = perf.mean_return_by_quantile(
        factor_data,
        by_group=False,
        demeaned=long_short,
        group_adjust=group_neutral,
    )

    mean_quant_rateret = mean_quant_ret.apply(
        utils.rate_of_return, axis=0, base_period=mean_quant_ret.columns[0]
    )

    mean_quant_ret_bydate, std_quant_daily = perf.mean_return_by_quantile(
        factor_data,
        by_date=True,
        by_group=False,
        demeaned=long_short,
        group_adjust=group_neutral,
    )

    mean_quant_rateret_bydate = mean_quant_ret_bydate.apply(
        utils.rate_of_return,
        axis=0,
        base_period=mean_quant_ret_bydate.columns[0],
    )

    compstd_quant_daily = std_quant_daily.apply(
        utils.std_conversion, axis=0, base_period=std_quant_daily.columns[0]
    )

    mean_ret_spread_quant, std_spread_quant = perf.compute_mean_returns_spread(
        mean_quant_rateret_bydate,
        factor_data["factor_quantile"].max(),
        factor_data["factor_quantile"].min(),
        std_err=compstd_quant_daily,
    )

    fr_cols = len(factor_returns.columns)
    vertical_sections = 2 + fr_cols * 3
    gf = GridFigure(rows=vertical_sections, cols=1)

    plotting.plot_quantile_returns_bar(
        mean_quant_rateret,
        by_group=False,
        ylim_percentiles=None,
        ax=gf.next_row(),
    )

    plotting.plot_quantile_returns_violin(
        mean_quant_rateret_bydate, ylim_percentiles=(1, 99), ax=gf.next_row()
    )

    trading_calendar = factor_data.index.levels[0].freq
    if trading_calendar is None:
        trading_calendar = pd.tseries.offsets.BDay()
        warnings.warn(
            "'freq' not set in factor_data index: assuming business day",
            UserWarning,
        )

    # Compute cumulative returns from daily simple returns, if '1D'
    # returns are provided.
    if "1D" in factor_returns:
        title = (
            "Factor Weighted "
            + ("Group Neutral " if group_neutral else "")
            + ("Long/Short " if long_short else "")
            + "Portfolio Cumulative Return (1D Period)"
        )

        plotting.plot_cumulative_returns(
            factor_returns["1D"], period="1D", title=title, ax=gf.next_row()
        )

        plotting.plot_cumulative_returns_by_quantile(
            mean_quant_ret_bydate["1D"], period="1D", ax=gf.next_row()
        )

    ax_mean_quantile_returns_spread_ts = [
        gf.next_row() for x in range(fr_cols)
    ]
    plotting.plot_mean_quantile_returns_spread_time_series(
        mean_ret_spread_quant,
        std_err=std_spread_quant,
        bandwidth=0.5,
        ax=ax_mean_quantile_returns_spread_ts,
    )

    if save_pic:
        plt.savefig(save_path)
    else:
        plt.show()
    gf.close()

    if by_group:
        (
            mean_return_quantile_group,
            mean_return_quantile_group_std_err,
        ) = perf.mean_return_by_quantile(
            factor_data,
            by_date=False,
            by_group=True,
            demeaned=long_short,
            group_adjust=group_neutral,
        )

        mean_quant_rateret_group = mean_return_quantile_group.apply(
            utils.rate_of_return,
            axis=0,
            base_period=mean_return_quantile_group.columns[0],
        )

        num_groups = len(
            mean_quant_rateret_group.index.get_level_values("group").unique()
        )

        vertical_sections = 1 + (((num_groups - 1) // 2) + 1)
        gf = GridFigure(rows=vertical_sections, cols=2)

        ax_quantile_returns_bar_by_group = [
            gf.next_cell() for _ in range(num_groups)
        ]
        plotting.plot_quantile_returns_bar(
            mean_quant_rateret_group,
            by_group=True,
            ylim_percentiles=(5, 95),
            ax=ax_quantile_returns_bar_by_group,
        )
        plt.show()
        gf.close()

