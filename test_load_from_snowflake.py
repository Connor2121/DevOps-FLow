requirements - unitycatalog-ai[databricks]

from unitycatalog.ai.core.databricks import DatabricksFunctionClient

_uc_client: DatabricksFunctionClient | None = None


def get_uc_function_client() -> DatabricksFunctionClient:
    """Return a cached DatabricksFunctionClient for invoking UC functions.

    Uses the app's service principal identity when deployed as a Databricks App,
    or the developer's identity when running locally.
    """
    global _uc_client
    if _uc_client is None:
        _uc_client = DatabricksFunctionClient()
    return _uc_client





# server/tools.py

from server import utils


def load_tools(mcp_server):
    # ... existing tools like health, get_current_user stay here ...

    @mcp_server.tool
    def get_recent_orders(customer_id: int, days_back: int = 30) -> dict:
        """
        Return recent orders for a customer within a given number of days.

        Use this tool when the user asks about a customer's order history,
        recent purchases, or the status of recent orders.

        Args:
            customer_id: The customer ID to look up orders for.
            days_back: How many days of history to include (default 30, max 365).

        Returns:
            dict: Contains a list of orders with order_id, order_date,
                  total_amount, and status fields.
        """
        if days_back < 1 or days_back > 365:
            return {"error": "days_back must be between 1 and 365"}

        client = utils.get_uc_function_client()
        result = client.execute_function(
            function_name="prod.sales.get_recent_orders",
            parameters={
                "customer_id": customer_id,
                "days_back": days_back,
            },
        )

        return {
            "customer_id": customer_id,
            "days_back": days_back,
            "orders": result.value,
        }
