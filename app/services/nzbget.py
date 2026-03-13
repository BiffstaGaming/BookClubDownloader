import base64
import logging
import requests

logger = logging.getLogger(__name__)


class NzbgetClient:
    def __init__(self, host: str, username: str, password: str):
        self.host = host.rstrip("/")
        self.username = username
        self.password = password

    def _rpc(self, method: str, params: list):
        """
        Make a JSON-RPC call to the NZBGet API.
        Raises ValueError if the response contains an error.
        Returns the 'result' field of a successful response.
        """
        url = f"{self.host}/jsonrpc"
        payload = {
            "version": "1.1",
            "method": method,
            "params": params,
        }
        try:
            response = requests.post(
                url,
                json=payload,
                auth=(self.username, self.password),
                timeout=30,
            )
            response.raise_for_status()
            data = response.json()
            if data.get("error"):
                error = data["error"]
                raise ValueError(f"NZBGet RPC error: {error}")
            return data.get("result")
        except requests.RequestException as exc:
            logger.error("NzbgetClient._rpc failed [%s]: %s", method, exc)
            raise

    def test_connection(self) -> bool:
        """Test the connection to NZBGet by calling the version method."""
        try:
            result = self._rpc("version", [])
            logger.info("NzbgetClient.test_connection: NZBGet version = %s", result)
            return True
        except Exception as exc:
            logger.warning("NzbgetClient.test_connection failed: %s", exc)
            return False

    def add_nzb(
        self,
        nzb_content: bytes,
        name: str,
        category: str = "",
        password: str = "",
    ) -> int:
        """
        Submit an NZB file to NZBGet.
        Returns the NZBGet job ID (integer).
        """
        # Ensure the name ends with .nzb
        safe_name = name if name.lower().endswith(".nzb") else f"{name}.nzb"

        # Base64-encode the NZB content
        b64_content = base64.b64encode(nzb_content).decode("utf-8")

        # Build post-processing parameters for the unpack password
        if password:
            pp_params = [["*unpack:password", password]]
        else:
            pp_params = []

        # NZBGet append parameters:
        # NZBFilename, Content, Category, Priority, AddToTop, AddPaused,
        # DupeKey, DupeScore, DupeMode, PPParameters
        params = [
            safe_name,      # NZBFilename
            b64_content,    # Content (base64)
            category,       # Category
            0,              # Priority (0 = normal)
            False,          # AddToTop
            False,          # AddPaused
            "",             # DupeKey
            0,              # DupeScore
            "SCORE",        # DupeMode
            pp_params,      # PPParameters
        ]

        result = self._rpc("append", params)
        job_id = int(result)
        logger.info("NzbgetClient.add_nzb: added '%s' with job ID %d", safe_name, job_id)
        return job_id
