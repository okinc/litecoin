/*
 * rpcaddress.cpp
 *
 *  Created on: 2014年10月28日
 *      Author: Administrator
 */
#include <stdexcept>

#include "main.h"
#include "bitcoinrpc.h"
#include "sync.h"
#include "key.h"
#include "base58.h"
#include "json/json_spirit_value.h"
#include "address-monitor/address-monitor.h"

using namespace json_spirit;
using namespace std;
using std::runtime_error;

json_spirit::Value addmonitor(const json_spirit::Array& params, bool fHelp)
{
    if (fHelp || params.size() == 0)
        throw runtime_error(
            "addmonitor \"bitcoinaddress\"\n"
            "\nadd a bitcoinaddress to monitor.\n"
            "\nResult\n"
            "\"bool\"      (string) true if not exsit; false if already exsit\n"
            "\nExamples\n"
        );

    json_spirit::Array ret;

    for(int i = 0; i < params.size(); i++)
    {
    	json_spirit::Array array;
    	json_spirit::Value param = params[i];
    	if(param.type() == str_type && !CBitcoinAddress().SetString(param.get_str()))
    	{
    		json_spirit::Value value;
    		read_string(param.get_str(), value);
    		array = value.get_array();
    	}
    	else if(param.type() == array_type)
    	{
    		json_spirit::Array tmpArray = param.get_array();
    		array.insert(array.end(), tmpArray.begin(), tmpArray.end());
    	}
    	else
    	{
    		array.push_back(param);
    	}

    	BOOST_FOREACH(json_spirit::Value value, array)
    	{
    		string strAddress = value.get_str();
    		CBitcoinAddress address;
    		if (!address.SetString(strAddress))
    			throw JSONRPCError(RPC_INVALID_ADDRESS_OR_KEY, "Invalid Bitcoin address");

    		uint160 addressKey;
    		if(!address.IsScript())
    		{
    			CKeyID keyID;
				if (!address.GetKeyID(keyID))
					throw JSONRPCError(RPC_TYPE_ERROR, "Address does not refer to a key");
				addressKey = keyID;
    		}
    		else
    		{
    			CScriptID cscriptID = boost::get<CScriptID>(address.Get());
    			addressKey = cscriptID;
    		}

    		LOCK(paddressMonitor->cs_address);

    		bool insertNew;
    		if(paddressMonitor->hasAddress(addressKey))
    		{
    			insertNew = false;
    		}
    		else
    		{
    			insertNew = paddressMonitor->AddAddress(addressKey, strAddress);
    		}

    		json_spirit::Object object;
    		object.push_back(Pair("address", strAddress));
    		object.push_back(Pair("ret", insertNew));
    		ret.push_back(object);
    	}
    }

    LOCK(paddressMonitor->cs_address);
    if(!paddressMonitor->Sync())
    {
    	throw runtime_error("Sync addmonitor fail!");
    }

    return ret;
}

json_spirit::Value delmonitor(const json_spirit::Array& params, bool fHelp)
{
    if (fHelp || params.size() == 0)
        throw runtime_error(
            "delmonitor \"bitcoinaddress\"\n"
            "\ndelete a bitcoinaddress monitored.\n"
            "\nResult\n"
            "\"bool\"      (string) true if exsit; false if not exsit\n"
            "\nExamples\n"
        );

    json_spirit::Array ret;

    for(int i = 0; i < params.size(); i++)
    {
    	json_spirit::Array array;
    	json_spirit::Value param = params[i];
    	if(param.type() == str_type && !CBitcoinAddress().SetString(param.get_str()))
		{
			json_spirit::Value value;
			read_string(param.get_str(), value);
			array = value.get_array();
		}
		else if(param.type() == array_type)
		{
			json_spirit::Array tmpArray = param.get_array();
			array.insert(array.end(), tmpArray.begin(), tmpArray.end());
		}
		else
		{
			array.push_back(param);
		}

    	BOOST_FOREACH(json_spirit::Value value, array)
    	{
    		string strAddress = value.get_str();
			CBitcoinAddress address;
			if (!address.SetString(strAddress))
				throw JSONRPCError(RPC_INVALID_ADDRESS_OR_KEY, "Invalid Bitcoin address");

    		uint160 addressKey;
    		if(!address.IsScript())
    		{
    			CKeyID keyID;
				if (!address.GetKeyID(keyID))
					throw JSONRPCError(RPC_TYPE_ERROR, "Address does not refer to a key");
				addressKey = keyID;
    		}
    		else
    		{
    			CScriptID cscriptID = boost::get<CScriptID>(address.Get());
    			addressKey = cscriptID;
    		}

			LOCK(paddressMonitor->cs_address);
			bool alreadyHas;
			if(!paddressMonitor->hasAddress(addressKey))
			{
				alreadyHas = false;
			}
			else
			{
				alreadyHas = paddressMonitor->DelAddress(addressKey, strAddress);
			}

			json_spirit::Object object;
			object.push_back(Pair("address", strAddress));
			object.push_back(Pair("ret", alreadyHas));
			ret.push_back(object);
    	}
    }

    LOCK(paddressMonitor->cs_address);
    if(!paddressMonitor->Sync())
    {
    	throw runtime_error("Sync delmonitor fail!");
    }

    return ret;
}

json_spirit::Value ismonitor(const json_spirit::Array& params, bool fHelp)
{
    if (fHelp || params.size() == 0)
        throw runtime_error(
            "ismonitor \"bitcoinaddress\"\n"
            "\test a bitcoinaddress is monitored.\n"
            "\nResult\n"
            "\"bool\"      (string) true if monitored; false if not\n"
            "\nExamples\n"
        );

    json_spirit::Array ret;

    for(int i = 0; i < params.size(); i++)
    {
    	json_spirit::Array array;
    	json_spirit::Value param = params[i];
    	if(param.type() == str_type && !CBitcoinAddress().SetString(param.get_str()))
		{
			json_spirit::Value value;
			read_string(param.get_str(), value);
			array = value.get_array();
		}
		else if(param.type() == array_type)
		{
			json_spirit::Array tmpArray = param.get_array();
			array.insert(array.end(), tmpArray.begin(), tmpArray.end());
		}
		else
		{
			array.push_back(param);
		}

    	BOOST_FOREACH(json_spirit::Value value, array)
    	{
    		string strAddress = value.get_str();
			CBitcoinAddress address;
			if (!address.SetString(strAddress))
				throw JSONRPCError(RPC_INVALID_ADDRESS_OR_KEY, "Invalid Bitcoin address");

    		uint160 addressKey;
    		if(!address.IsScript())
    		{
    			CKeyID keyID;
				if (!address.GetKeyID(keyID))
					throw JSONRPCError(RPC_TYPE_ERROR, "Address does not refer to a key");
				addressKey = keyID;
    		}
    		else
    		{
    			CScriptID cscriptID = boost::get<CScriptID>(address.Get());
    			addressKey = cscriptID;
    		}

			LOCK(paddressMonitor->cs_address);

			json_spirit::Object object;
			object.push_back(Pair("address", strAddress));
			object.push_back(Pair("ret", paddressMonitor->hasAddress(addressKey)));
			ret.push_back(object);
    	}
    }

    return ret;
}

json_spirit::Value ackmonitor(const json_spirit::Array& params, bool fHelp)
{
    if (fHelp || params.size() != 1)
        throw runtime_error(
            "ackmonitor \"requestId\"\n"
            "\ack a monitor request.\n"
            "\nResult\n"
            "\"bool\"      (string) true if success\n"
            "\nExamples\n"
        );

    json_spirit::Array ret;

    json_spirit::Value param = params[0];
    const string requestId = param.get_str();

    return paddressMonitor->ack(requestId);
}


//
// Utilities: convert hex-encoded Values
// (throws error if not hex).
//
static uint256 ParseHashV(const Value& v, string strName)
{
    string strHex;
    if (v.type() == str_type)
        strHex = v.get_str();
    if (!IsHex(strHex)) // Note: IsHex("") is false
        throw JSONRPCError(RPC_INVALID_PARAMETER, strName+" must be hexadecimal string (not '"+strHex+"')");
    uint256 result;
    result.SetHex(strHex);
    return result;
}

json_spirit::Value resynctx(const json_spirit::Array& params, bool fHelp)
{
    if (fHelp || params.size() != 1)
        throw runtime_error(
            "resynctx \"txId\"\n"
            "\re-sync a tx.\n"
            "\nResult\n"
            "\"bool\"      (string) true if confirms > 0\n"
            "\nExamples\n"
        );

    uint256 txId = ParseHashV(params[0], "parameter 1");

    bool fVerbose = false;
    if (params.size() > 1)
        fVerbose = (params[1].get_int() != 0);

    CTransaction tx;
    uint256 hashBlock = 0;
    if (!GetTransaction(txId, tx, hashBlock, true))
    {
        throw JSONRPCError(RPC_INVALID_ADDRESS_OR_KEY, "No information available about transaction");
    }

    const bool confirmed = hashBlock == 0;

    LOCK(paddressMonitor->cs_address);

    paddressMonitor->SyncTransaction(txId, tx, NULL);
    if(confirmed)
    {
        if (mapBlockIndex.count(hashBlock) == 0)
        {
            throw JSONRPCError(RPC_INVALID_ADDRESS_OR_KEY, "Block not found");
        }

        CBlock block;
        CBlockIndex* pblockindex = mapBlockIndex[hashBlock];

        if(!block.ReadFromDisk(pblockindex))
        {
            throw JSONRPCError(RPC_INTERNAL_ERROR, "Can't read block from disk");
        }

    	paddressMonitor->SyncConnectBlock(&block, pblockindex, tx);
    }

    return confirmed;
}


