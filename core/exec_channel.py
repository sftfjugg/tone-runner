from core.agent import star_agent
from core.agent import tone_agent
from core.exception import ExecChanelException, AgentRequestException
from constant import ChannelType
from tools.log_util import LoggerFactory


logger = LoggerFactory.exec_channel()
summary_logger = LoggerFactory.summary_error_log()


class ExecChannel:

    @classmethod
    def star_agent_convert_kwargs(cls, kwargs):
        ip = kwargs.get("ip")
        tid, sync = kwargs.get("tid"), kwargs.get("sync")
        if "ip" in kwargs:
            kwargs.pop("ip")
            kwargs["ip_list"] = [ip]
        if "sn" in kwargs:
            kwargs.pop("sn")
            kwargs["sn_list"] = [ip]
        if "tid" in kwargs:
            kwargs.pop("tid")
            kwargs["uid_list"] = [tid]
        if "env" in kwargs:
            kwargs.pop("env")
        if "sync" in kwargs:
            kwargs["sync"] = "true" if sync else "false"

    @classmethod
    def tone_agent_convert_kwargs(cls, kwargs):
        sync = kwargs.get("sync")
        if "sync" in kwargs:
            kwargs["sync"] = "true" if sync else "false"

    @classmethod
    def cloud_agent_convert_kwargs(cls, kwargs):
        sync = kwargs.get("sync")
        if sync:
            kwargs.pop("sync")
            kwargs["is_sync"] = sync

    @classmethod
    def star_agent_convert_kwargs_for_do_stop(cls, kwargs):
        if "tid" in kwargs:
            kwargs["uid"] = kwargs["tid"]
            kwargs.pop("tid")

    @classmethod
    def tone_agent_convert_kwargs_for_do_stop(cls, kwargs):
        kwargs.pop("ip")
        kwargs.pop("sn")

    @classmethod
    def cloud_agent_convert_kwargs_for_do_stop(cls, kwargs):
        kwargs.pop("sn")

    @classmethod
    def do_exec(cls, channel_type, **kwargs):
        logger.info(f"kwargs of do_exec with {channel_type} is: {kwargs}")
        try:
            if channel_type == ChannelType.STAR_AGENT:
                cls.star_agent_convert_kwargs(kwargs)
                success, result = star_agent.do_exec(**kwargs)
                if isinstance(result, list):
                    result = result[0]
                logger.info(f"the {channel_type} do_exec success: {success}, result: {result}")
                return success, result
            elif channel_type == ChannelType.TONE_AGENT:
                cls.tone_agent_convert_kwargs(kwargs)
                success, result = tone_agent.do_exec(**kwargs)
                logger.info(f"the {channel_type} do_exec success: {success}, result: {result}")
                return success, result
            else:
                error_msg = f"Unsupported channel type: {channel_type}, do_exec info:{kwargs}"
                logger.error(error_msg)
                raise ExecChanelException(error_msg)
        except Exception as error:
            logger.exception(f"do_exec has exception for {channel_type}, do_exec info: {kwargs}")
            summary_logger.exception(f"do_exec has exception for {channel_type}, do_exec info: {kwargs}")
            raise ExecChanelException(error)

    @classmethod
    def do_query(cls, channel_type, **kwargs):
        logger.info(f"kwargs of do_query with {channel_type} is: {kwargs}")
        try:
            if channel_type == ChannelType.STAR_AGENT:
                cls.star_agent_convert_kwargs(kwargs)
                success, result = star_agent.do_query(**kwargs)
                if not success:
                    logger.error(f"the {channel_type} do_query result: {result}")
                    raise AgentRequestException("star-agent do_query request fail.")
                result = result[0] if isinstance(result, list) else result
                logger.info(f"the {channel_type} do_query result: {result}")
                return result
            elif channel_type == ChannelType.TONE_AGENT:
                result = tone_agent.do_query(**kwargs)
                logger.info(f"the {channel_type} do_query result: {result}")
                return result
            else:
                error_msg = f"Unsupported channel type: {channel_type}, do_query info:{kwargs}"
                logger.error(error_msg)
                raise ExecChanelException(error_msg)
        except Exception as error:
            logger.exception(f"do_query has exception for {channel_type}, do_query info: {kwargs}")
            summary_logger.exception(f"do_query has exception for {channel_type}, do_query info: {kwargs}")
            raise ExecChanelException(error)

    @classmethod
    def do_stop(cls, channel_type, **kwargs):
        logger.info(f"kwargs of do_stop with {channel_type} is: {kwargs}")
        try:
            if channel_type == ChannelType.STAR_AGENT:
                cls.star_agent_convert_kwargs_for_do_stop(kwargs)
                success, result = star_agent.do_stop(**kwargs)
                result = result[0] if isinstance(result, list) else result
                logger.info(f"the {channel_type} do_stop is_success: {success}, result: {result}")
                return success, result
            elif channel_type == ChannelType.TONE_AGENT:
                cls.tone_agent_convert_kwargs_for_do_stop(kwargs)
                success, result = tone_agent.do_stop(**kwargs)
                logger.info(f"the {channel_type} do_stop is_success: {success}, result: {result}")
                return success, result
            else:
                error_msg = f"Unsupported channel type: {channel_type}, do_stop info:{kwargs}"
                logger.error(error_msg)
                raise ExecChanelException(error_msg)
        except Exception as error:
            logger.exception(f"do_stop has exception for {channel_type}, do_stop info: {kwargs}")
            summary_logger.exception(f"do_stop has exception for {channel_type}, do_stop info: {kwargs}")
            raise ExecChanelException(error)

    @classmethod
    def check_server_status(cls, channel_type, server_ip, **kwargs):
        error_msg = ""
        logger.info(f"{server_ip} check server status, it's channel_type is {channel_type}")
        try:
            if channel_type == ChannelType.STAR_AGENT:
                check_res, error_msg = star_agent.check_server_status(server_ip, **kwargs)
            elif channel_type == ChannelType.TONE_AGENT:
                check_res, error_msg = tone_agent.check_server_status(server_ip, **kwargs)
            else:
                check_res = False
                logger.error(f"Unsupported channel type: {channel_type}, it's ip is: {server_ip}")
            if not check_res:
                logger.error(f"server<{server_ip}> check status fail, it's channel type is: {channel_type}")
            return check_res, error_msg
        except Exception as error:
            logger.exception("check server status has exception: ")
            summary_logger.exception("check server status has exception: {}".format(error))
            raise ExecChanelException(error)

    @classmethod
    def check_reboot(cls, channel_type, server_ip, **kwargs):
        logger.info(f"{server_ip} check server reboot, it's channel_type is {channel_type}")
        try:
            if channel_type == ChannelType.STAR_AGENT:
                check_res = star_agent.check_reboot(server_ip, **kwargs)
            elif channel_type == ChannelType.TONE_AGENT:
                check_res = tone_agent.check_reboot(server_ip, **kwargs)
            else:
                check_res = False
                logger.error(f"Unsupported channel type: {channel_type}, it's ip is: {server_ip}")
            if not check_res:
                logger.error(f"server<{server_ip}> check reboot fail, it's channel type is: {channel_type}")
            return check_res
        except Exception as error:
            logger.exception("check server reboot has exception: ")
            summary_logger.exception("check server reboot has exception: {}".format(error))
            raise ExecChanelException(error)
