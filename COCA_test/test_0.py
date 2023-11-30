import COCA_Broker
import COCA_Archer


def test_list_flights(addr, usr, pwd):
    """
    This function tests the list_flights method.
    """
    # Create a broker instance with the given configuration file and debug level
    broker = COCA_Broker.get_broker('test_config.json', 'DEBUG')

    # Get a client instance using the provided address, username, and password
    c, t = COCA_Archer.get_client(addr, usr, pwd)
