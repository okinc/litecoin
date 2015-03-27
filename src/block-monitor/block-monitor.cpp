/*
 * block-monitor.cpp
 *
 *  Created on: 2015年3月26日
 *      Author: Administrator
 */


#include "block-monitor/block-monitor.h"
#include "serialize.h"
#include "util.h"
#include "base58.h"
#include "script.h"
#include "main.h"
#include "bitcoinrpc.h"

#include <sstream>

#include <boost/lexical_cast.hpp>
#include <boost/algorithm/string/replace.hpp>
#include <boost/filesystem.hpp>
#include <boost/filesystem/fstream.hpp>
#include <boost/foreach.hpp>
#include <boost/asio.hpp>
#include <boost/asio/ssl.hpp>
#include <boost/iostreams/concepts.hpp>
#include <boost/iostreams/stream.hpp>
#include <boost/asio/io_service.hpp>

#include "json/json_spirit_value.h"
#include "json/json_spirit_reader_template.h"
#include "json/json_spirit_writer_template.h"
#include "json/json_spirit_utils.h"

using namespace std;
using namespace boost;
using namespace json_spirit;
using namespace boost::asio;
using boost::lexical_cast;
using std::tr1::unordered_map;

static boost::asio::io_service ioService;
static boost::asio::io_service::work threadPool(ioService);

static void io_service_run(void)
{
	ioService.run();
}


BlockMonitor::BlockMonitor(size_t nCacheSize, bool fMemory, bool fWipe) :
	CLevelDB(GetDataDir() / "blocks" / "blockmon", nCacheSize, fMemory, fWipe),
	retryDelay(0), httpPool(BLOCKMON_HTTP_POOL), sem_post(0), sem_acked(0), sem_resend(0), is_stop(false)
{
	retryDelay = GetArg("-blockmon_retry_delay", BLOCKMON_RETRY_DELAY);
	httpPool = GetArg("-blockmon_http_pool", BLOCKMON_HTTP_POOL);
}

bool BlockMonitor::LoadBlocks()
{
	leveldb::Iterator *pcursor = NewIterator();

	CDataStream ssKeySet(SER_DISK, CLIENT_VERSION);
	ssKeySet << make_pair('B', make_pair(int64_t(0), uint256(0)));
	pcursor->Seek(ssKeySet.str());

	queue<pair<pair<int64_t, uint256>, pair<int, string> > > syncConnectQueue;
	queue<pair<pair<int64_t, uint256>, pair<int, string> > > syncDisconnectQueue;

	//Load addresses
	while(pcursor->Valid())
	{
		boost::this_thread::interruption_point();
		try
		{
			leveldb::Slice slKey = pcursor->key();
			CDataStream ssKey(slKey.data(), slKey.data()+slKey.size(), SER_DISK, CLIENT_VERSION);
			char chType;
			ssKey >> chType;
			if(chType == 'B')
			{
				leveldb::Slice slValue = pcursor->value();
				CDataStream ssValue(slValue.data(), slValue.data()+slValue.size(), SER_DISK, CLIENT_VERSION);

				int64_t timestamp;
				ssKey >> timestamp;

				uint256 uuid;
				ssKey >> uuid;

				int type;
				ssValue >> type;

				string json;
				ssValue >> json;

				if(type == SYNC_CONNECT)
				{
					syncConnectQueue.push(make_pair(make_pair(timestamp, uuid), make_pair(type, json)));
				}
				else if(type == SYNC_DISCONNECT)
				{
					syncDisconnectQueue.push(make_pair(make_pair(timestamp, uuid), make_pair(type, json)));
				}
				else
				{
					throw runtime_error("unknow type: "+lexical_cast<string>(type));
				}
			}
			else
			{
				break;
			}

			pcursor->Next();
		}
		catch(std::exception &e)
		{
			throw runtime_error(strprintf("%s : Deserialize or I/O error - %s", __func__, e.what()));
		}
	}
	delete pcursor;

	threadGroup.create_thread(boost::bind(&BlockMonitor::LoadSyncConnect, this, syncConnectQueue));
	threadGroup.create_thread(boost::bind(&BlockMonitor::LoadSyncDisconnect, this, syncDisconnectQueue));

	return true;
}


bool BlockMonitor::WriteBlock(const int64_t &timestamp, const uint256 &uuid, const int type, const std::string &json)
{
	return Write(std::make_pair('B', std::make_pair(timestamp, uuid)), std::make_pair(type, json), true);
}

bool BlockMonitor::DeleteBlock(const int64_t &timestamp, const uint256 &uuid)
{
	return Erase(std::make_pair('B', std::make_pair(timestamp, uuid)), true);
}

void BlockMonitor::Load()
{
	if(!LoadBlocks())
	{
		throw runtime_error("BlockMonitor LoadTransactions fail!");
	}

	for(int i = 0; i < httpPool; i++)
	{
		threadGroup.create_thread(boost::bind(&io_service_run));
	}

    threadGroup.create_thread(boost::bind(&BlockMonitor::PostThread, this));
    threadGroup.create_thread(boost::bind(&BlockMonitor::AckThread, this));
    threadGroup.create_thread(boost::bind(&BlockMonitor::ResendThread, this));
    threadGroup.create_thread(boost::bind(&BlockMonitor::NoResponseCheckThread, this));
}

static json_spirit::Object buildValue(const CBlock *pblock,
		const string &blockHash, const int nHeight, const int64_t &time, const int status)
{
	json_spirit::Object object;

	object.push_back(Pair("time", time));
	object.push_back(Pair("coinType", 2));
	if(pblock)
	{
		object.push_back(Pair("blockHeight", nHeight));
		object.push_back(Pair("blockHash", blockHash));
	}
	object.push_back(Pair("status", status));

	return object;
}

void BlockMonitor::SyncConnectBlock(const CBlock *pblock, CBlockIndex* pindex)
{
	json_spirit::Object ret;
	int64_t now = 0;

	{
		now = GetAdjustedTime();
		const string blockHash = pblock->GetHash().GetHex();
		const int nHeight = pindex->nHeight;
		const int status = 1;

		ret = buildValue(pblock, blockHash, nHeight, now, status);
	}

	uint256 uuid = NewRandomUUID();
	string requestId = "conn-" + NewRequestId(now, uuid);

	json_spirit::Object result;
	result.push_back(Pair("requestId", requestId));
	result.push_back(Pair("content", ret));
	string json = write_string(Value(result), false);

	if(!WriteBlock(now, uuid, SYNC_CONNECT, json))
	{
		//TODO
	}

	push_post(requestId, json);
}

void BlockMonitor::SyncDisconnectBlock(const CBlock *pblock)
{
	json_spirit::Object ret;
	int64_t now = 0;

	{
		now = GetAdjustedTime();
		string blockHash = pblock->GetHash().GetHex();
		int nHeight = -2;
		const int status = -2;

		ret = buildValue(pblock, blockHash, nHeight, now, status);
	}

	uint256 uuid = NewRandomUUID();
	string requestId = "dis-" + NewRequestId(now, uuid);

	json_spirit::Object result;
	result.push_back(Pair("requestId", requestId));
	result.push_back(Pair("content", ret));
	string json = write_string(Value(result), false);

	if(!WriteBlock(now, uuid, SYNC_DISCONNECT, json))
	{
		//TODO
	}

	push_post(requestId, json);
}

bool BlockMonitor::ack(const string &requestId)
{
	push_acked(requestId);
	return true;
}

const uint256 BlockMonitor::NewRandomUUID() const
{
	uint256 uuid;
	RandAddSeedPerfmon();

	RAND_bytes(uuid.begin(), uuid.end() - uuid.begin());
	return uuid;
}

const std::string BlockMonitor::NewRequestId(const int64_t &now, const uint256 &uuid) const
{
	CDataStream ssId(SER_DISK, CLIENT_VERSION);
	ssId << now;
	ssId << uuid;

	return EncodeBase58((const unsigned char*)&ssId[0], (const unsigned char*)&ssId[0] + (int)ssId.size());
}

bool BlockMonitor::decodeRequestIdWithoutPrefix(const std::string &requestIdWithoutPrefix, int64_t &now, uint256 &uuid)
{
	std::vector<unsigned char> vch;
	if(!DecodeBase58(requestIdWithoutPrefix, vch))
	{
		return false;
	}

	CDataStream ssId(vch, SER_DISK, CLIENT_VERSION);

	ssId >> now;
	ssId >> uuid;

	return true;
}

bool BlockMonitor::decodeRequestIdWitPrefix(const std::string &requestIdWithPrefix, int64_t &now, uint256 &uuid)
{
	if(requestIdWithPrefix.substr(0, 5) == "conn-")
	{
		return decodeRequestIdWithoutPrefix(requestIdWithPrefix.substr(5), now, uuid);
	}
	else if(requestIdWithPrefix.substr(0, 5) == "dis-")
	{
		return decodeRequestIdWithoutPrefix(requestIdWithPrefix.substr(4), now, uuid);
	}
	else
	{
		return false;
	}
}

const std::string BlockMonitor::NewRequestId() const
{
	const int64_t now = GetAdjustedTime();
	const uint256 uuid = NewRandomUUID();

	return NewRequestId(now, uuid);
}

void BlockMonitor::push_post(const std::string &requestId, const std::string &json)
{
	LOCK2(cs_map, cs_post);

	requestMap.insert(make_pair(requestId, json));
	postQueue.push(requestId);

	sem_post.post();
}

void BlockMonitor::push_acked(const std::string &requestId)
{
	LOCK(cs_acked);

	ackedQueue.push(requestId);

	sem_acked.post();
}

void BlockMonitor::push_resend(const std::string &requestId)
{
	LOCK(cs_resend);

	resendQueue.push(make_pair(requestId, GetAdjustedTime() + retryDelay));

	sem_resend.post();
}

bool BlockMonitor::pull_post(std::string &requestId, const std::string ** const ppjson)
{
	sem_post.wait();
	if(is_stop)
	{
		return false;
	}

	{
		LOCK2(cs_map, cs_post);

		requestId = postQueue.front();
		postQueue.pop();

		std::tr1::unordered_map<string, string>::const_iterator it = requestMap.find(requestId);
		if(it == requestMap.end())
		{
			throw runtime_error("pull_post can not find request in map: "+requestId);
		}

		*ppjson = &it->second;
	}

	{
		LOCK(cs_postMap);
		postMap.insert(make_pair(requestId, GetAdjustedTime() + retryDelay));
	}

	return true;
}

bool BlockMonitor::pull_acked(std::string &requestId, const std::string ** const ppjson)
{
	sem_acked.wait();
	if(is_stop)
	{
		return false;
	}

	LOCK2(cs_map, cs_acked);

	requestId = ackedQueue.front();
	ackedQueue.pop();

	std::tr1::unordered_map<string, std::string>::const_iterator it = requestMap.find(requestId);
	if(it == requestMap.end())
	{
		LogBlock("pull_acked can not find request in map: "+requestId+"\n");
		return false;
	}

	*ppjson = &it->second;

	return true;
}

bool BlockMonitor::pull_resend(std::string &requestId, const std::string ** const ppjson)
{
	if(!sem_resend.try_wait())
	{
		return false;
	}
	if(is_stop)
	{
		return false;
	}

	LOCK2(cs_map, cs_resend);

	const int64_t now = GetAdjustedTime();

	for(int i = 0; i < 100; i++)
	{
		pair<std::string, int64_t> requestAndTime = resendQueue.top();
		if(requestAndTime.second > now)
		{
			sem_resend.post();
			return false;
		}

		resendQueue.pop();

		requestId = requestAndTime.first;

		std::tr1::unordered_map<string, string>::const_iterator it = requestMap.find(requestId);
		if(it == requestMap.end())
		{
			if(resendQueue.empty())
			{
				return false;
			}
			else
			{
				continue;
			}
		}

		*ppjson = &it->second;

		resendQueue.push(make_pair(requestId, now + retryDelay));

		sem_resend.post();

		return true;
	}

	return false;
}

//
// IOStream device that speaks SSL but can also speak non-SSL
//
template <typename Protocol>
class SSLIOStreamDevice : public iostreams::device<iostreams::bidirectional> {
public:
    SSLIOStreamDevice(asio::ssl::stream<typename Protocol::socket> &streamIn, bool fUseSSLIn) : stream(streamIn)
    {
        fUseSSL = fUseSSLIn;
        fNeedHandshake = fUseSSLIn;
    }

    void handshake(ssl::stream_base::handshake_type role)
    {
        if (!fNeedHandshake) return;
        fNeedHandshake = false;
        stream.handshake(role);
    }
    std::streamsize read(char* s, std::streamsize n)
    {
        handshake(ssl::stream_base::server); // HTTPS servers read first
        if (fUseSSL) return stream.read_some(asio::buffer(s, n));
        return stream.next_layer().read_some(asio::buffer(s, n));
    }
    std::streamsize write(const char* s, std::streamsize n)
    {
        handshake(ssl::stream_base::client); // HTTPS clients write first
        if (fUseSSL) return asio::write(stream, asio::buffer(s, n));
        return asio::write(stream.next_layer(), asio::buffer(s, n));
    }
    bool connect(const std::string& server, const std::string& port)
    {
        ip::tcp::resolver resolver(stream.get_io_service());
        ip::tcp::resolver::query query(server.c_str(), port.c_str());
        ip::tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
        ip::tcp::resolver::iterator end;
        boost::system::error_code error = asio::error::host_not_found;
        while (error && endpoint_iterator != end)
        {
            stream.lowest_layer().close();
            stream.lowest_layer().connect(*endpoint_iterator++, error);
        }
        if (error)
            return false;
        return true;
    }

private:
    bool fNeedHandshake;
    bool fUseSSL;
    asio::ssl::stream<typename Protocol::socket>& stream;
};

static void CallRPC(BlockMonitor* self, const std::string &requestId, const string& body)
{
    const bool fUseSSL = false;
    asio::io_service io_service;
    ssl::context context(io_service, ssl::context::sslv23);
    context.set_options(ssl::context::no_sslv2);
    asio::ssl::stream<asio::ip::tcp::socket> sslStream(io_service, context);
    SSLIOStreamDevice<asio::ip::tcp> d(sslStream, fUseSSL);
    iostreams::stream< SSLIOStreamDevice<asio::ip::tcp> > stream(d);

    const bool fWait = false;
    const string host = GetArg("-blockmon_host", "127.0.0.1");
    const string port = GetArg("-blockmon_port", "80");
    const string url = GetArg("-blockmon_url", "");
    do {
        bool fConnected = d.connect(host, port);
        if (fConnected) break;
        if (fWait)
            MilliSleep(1000);
        else
        	LogBlock("couldn't connect to server\n");
    } while (fWait);

    // HTTP basic authentication
    map<string, string> mapRequestHeaders;

    string strPost = HTTPPost(body, mapRequestHeaders, host, url);
    stream << strPost << std::flush;

    // Receive HTTP reply status
    int nProto = 0;
    int nStatus = ReadHTTPStatus(stream, nProto);

    // Receive HTTP reply message headers and body
    map<string, string> mapHeaders;
    string strReply;
    ReadHTTPMessage(stream, mapHeaders, strReply, nProto);

    if (nStatus == HTTP_UNAUTHORIZED)
    	LogBlock("incorrect rpcuser or rpcpassword (authorization failed)\n");
    else if (nStatus >= 400 && nStatus != HTTP_BAD_REQUEST && nStatus != HTTP_NOT_FOUND && nStatus != HTTP_INTERNAL_SERVER_ERROR)
    	LogBlock("server returned HTTP error %d"+ boost::lexical_cast<string>(nStatus)+"\n");
    else if (strReply.empty())
    	LogBlock("no response from server\n");

    if(strReply.empty())
    {
    	LogBlock("empty reply -> requestId: "+requestId+"\n");
    }
    else if(strReply == "true")
    {
    	self->ack(requestId);
    	LogBlock("success reply -> requestId: "+requestId+"\n");
    }
    else
    {
    	LogBlock("wrong reply -> requestId: "+requestId+", reply: "+strReply+"\n");
    }

    // Parse reply
//    Value valReply;
//    if (!read_string(strReply, valReply))
//    	LogBlock("couldn't parse reply from server\n");
//    const Object& reply = valReply.get_obj();
//    if (reply.empty())
//    	LogBlock("expected reply to have result, error and id properties\n");
//
//    cout << "reply: " << strReply << endl;
//
//    return reply;
}

static void CallRPCWrappedException(BlockMonitor* self, const std::string &requestId, const string& body)
{
	try
	{
		CallRPC(self, requestId, body);
	}
	catch(const std::exception& e)
	{
		LogBlock("exception in CallRPC -> "+string(e.what())+"\n");
	}
	catch(...)
	{
		LogBlock("unknow exception in CallRPC\n");
	}
}

void BlockMonitor::PostThread()
{
    RenameThread("bitcoin-block-monitor-post");

    static bool fOneThread;
    if (fOneThread)
    {
        return;
    }
    fOneThread = true;

    MilliSleep(retryDelay * 1000);

	while(!is_stop)
	{
		string requestId;
		const string * pjson;

		if(!pull_post(requestId, &pjson))
		{
			MilliSleep(1000);
			continue;
		}

		try
		{
			do_post(requestId, pjson);
		}
		catch(std::exception &e)
		{
	    	LogException(&e, string("BlockMonitor::PostThread() -> "+string(e.what())).c_str());
	    	LogBlock("BlockMonitor::PostThread() -> "+string(e.what())+"\n");
		}
		catch(...)
		{
			LogBlock("BlockMonitor::PostThread() -> unknow exception\n");
		}
	}
}

void BlockMonitor::AckThread()
{
    RenameThread("bitcoin-block-monitor-ack");

    static bool fOneThread;
    if (fOneThread)
    {
        return;
    }
    fOneThread = true;

    MilliSleep(retryDelay * 1000);

	while(!is_stop)
	{
		string requestId;
		const string * pjson;
		string json;

		if(!pull_acked(requestId, &pjson))
		{
			MilliSleep(1000);
			continue;
		}
		json = *pjson;

		try
		{
			if(do_acked(requestId))
				LogBlock("do_acked success -> requestId: "+requestId+", json: "+json+"\n");
			else
				LogBlock("do_acked fail -> requestId: "+requestId+", json: "+json+"\n");
		}
		catch(std::exception &e)
		{
	    	LogException(&e, string("BlockMonitor::AckThread() -> "+string(e.what())).c_str());
	    	LogBlock("BlockMonitor::AckThread() -> "+string(e.what())+"\n");
		}
		catch(...)
		{
			LogBlock("BlockMonitor::AckThread() -> unknow exception\n");
		}
	}
}

void BlockMonitor::ResendThread()
{
    RenameThread("bitcoin-block-monitor-resend");

    static bool fOneThread;
    if (fOneThread)
    {
        return;
    }
    fOneThread = true;

    MilliSleep(retryDelay * 1000);

	while(!is_stop)
	{
		string requestId;
		const string * pjson;

		if(!pull_resend(requestId, &pjson))
		{
			MilliSleep(1000);
			continue;
		}

		try
		{
			do_resend(requestId, pjson);
		}
		catch(std::exception &e)
		{
	    	LogException(&e, string("BlockMonitor::ResendThread() -> "+string(e.what())).c_str());
	    	LogBlock("BlockMonitor::ResendThread() -> "+string(e.what())+"\n");
		}
		catch(...)
		{
			LogBlock("BlockMonitor::ResendThread() -> unknow exception\n");
		}
	}
}

void BlockMonitor::NoResponseCheckThread()
{
    RenameThread("bitcoin-block-monitor-NoResponseCheck");

    static bool fOneThread;
    if (fOneThread)
    {
        return;
    }
    fOneThread = true;

    MilliSleep(retryDelay * 1000);

	while(true)
	{
		try
		{
			NoResponseCheck();
		}
		catch(std::exception &e)
		{
	    	LogException(&e, string("BlockMonitor::NoResponseCheckThread() -> "+string(e.what())).c_str());
	    	LogBlock("BlockMonitor::NoResponseCheckThread() -> "+string(e.what())+"\n");
		}
		catch(...)
		{
			LogBlock("BlockMonitor::NoResponseCheckThread() -> unknow exception\n");
		}

		MilliSleep(retryDelay * 1000);
	}
}

bool BlockMonitor::do_post(const std::string &requestId, const std::string * pjson)
{
	LogBlock("do_post -> requestId: "+requestId+", json: "+*pjson+"\n");
	ioService.post(boost::bind(CallRPCWrappedException, this, requestId, *pjson));
	return true;
}

bool BlockMonitor::do_acked(const std::string &requestId)
{
	int64_t timestamp;
	uint256 uuid;
	if(!decodeRequestIdWitPrefix(requestId, timestamp, uuid))
	{
		return false;
	}

	{
		LOCK(cs_postMap);
		postMap.erase(requestId);
	}

	{
		LOCK(cs_map);
		requestMap.erase(requestId);
	}

	return DeleteBlock(timestamp, uuid);
}

bool BlockMonitor::do_resend(const std::string &requestId, const std::string * pjson)
{
	LogBlock("do_resend -> requestId: "+requestId+", json: "+*pjson+"\n");
	ioService.post(boost::bind(CallRPCWrappedException, this, requestId, *pjson));
	return true;
}

void BlockMonitor::NoResponseCheck()
{
	vector<string> timeoutRequestIds;

	{
		LOCK(cs_postMap);
		int64_t now = GetAdjustedTime();

		for(std::tr1::unordered_map<string, int64_t>::const_iterator it = postMap.begin(); it != postMap.end(); ++it)
		{
			if(it->second < now)
			{
				timeoutRequestIds.push_back(it->first);
			}
		}

		BOOST_FOREACH(const string &requestId, timeoutRequestIds)
		{
			postMap.erase(requestId);
		}
	}

	{
		LOCK(cs_resend);
		int64_t now = GetAdjustedTime();

		BOOST_FOREACH(const string &requestId, timeoutRequestIds)
		{
			resendQueue.push(make_pair(requestId, now));
			sem_resend.post();
		}
	}
}

void BlockMonitor::LoadSyncConnect(queue<pair<pair<int64_t, uint256>, pair<int, string> > > &syncConnectQueue)
{
    RenameThread("bitcoin-block-monitor-LoadSyncConnect");

    static bool fOneThread;
    if (fOneThread)
    {
        return;
    }
    fOneThread = true;

    MilliSleep(retryDelay * 1000);

	while(!syncConnectQueue.empty())
    {
		pair<pair<int64_t, uint256>, pair<int, string> > request = syncConnectQueue.front();
		syncConnectQueue.pop();
		const string &json = request.second.second;
		const int64_t &now = request.first.first;
		const uint256 &uuid = request.first.second;
		const string requestId = "conn-" + NewRequestId(now, uuid);

		push_post(requestId, json);
    }
}

void BlockMonitor::LoadSyncDisconnect(queue<pair<pair<int64_t, uint256>, pair<int, string> > > &syncDisconnectQueue)
{
    RenameThread("bitcoin-block-monitor-LoadSyncDisconnect");

    static bool fOneThread;
    if (fOneThread)
    {
        return;
    }
    fOneThread = true;

    MilliSleep(retryDelay * 1000);

	while(!syncDisconnectQueue.empty())
    {
		pair<pair<int64_t, uint256>, pair<int, string> > request = syncDisconnectQueue.front();
		syncDisconnectQueue.pop();
		const string &json = request.second.second;
		const int64_t &now = request.first.first;
		const uint256 &uuid = request.first.second;
		const string requestId = "dis-" + NewRequestId(now, uuid);

		push_post(requestId, json);
    }
}

void BlockMonitor::Stop()
{
	is_stop = true;
	sem_post.post();
	sem_acked.post();
	sem_resend.post();

	ioService.stop();
	threadGroup.interrupt_all();
	threadGroup.join_all();
}
