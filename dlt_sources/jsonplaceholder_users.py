import dlt
import requests

@dlt.source(name="jsonplaceholder")
def jsonplaceholder_users_source(
    api_url="https://jsonplaceholder.typicode.com/users",
):
    """
    A dlt source to retrieve user data from the JSONPlaceholder API.
    """
    @dlt.resource(write_disposition="replace", name="users")
    def users_resource():
        """Retrieves a list of users."""
        response = requests.get(api_url)
        response.raise_for_status()
        yield response.json()

    return users_resource()
