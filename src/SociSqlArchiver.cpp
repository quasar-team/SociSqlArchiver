/*
 * SociSqlArchiver.cpp
 *
 *  Created on: 30 Aug 2017
 *      Author: pnikiel
 */

#include <SociSqlArchiver.h>
#include <ASUtils.h>
#include <LogIt.h>
#include <uavariant.h>
#include <boost/lexical_cast.hpp>
#include <ctime>

namespace SociSqlArchiver
{

ArchivedItem::ArchivedItem( const std::string& attribute, const std::string& addr, const std::string& value ):
        m_attribute(attribute),
        m_address(addr),
        m_value(value)
{
        std::time_t now = time(0);
        m_timestamp = *localtime(&now);
}

SociSqlArchiver::SociSqlArchiver(const std::string& sociAddress):
        m_session(sociAddress),
        m_isRunning(true)
{
    // singleton: let only one specific archiver in the system
    if (s_instance)
        abort_with_message(__FILE__,__LINE__,"Trying to create more then one specific archiver objects. Can't - it's a singleton.");

    s_instance = this;

    m_archiverThread = boost::thread([this](){ this->archivingThread(); });
}

SociSqlArchiver::~SociSqlArchiver()
{
    // TODO
}

void SociSqlArchiver::archiveAssignment (
         const UaNodeId& objectAddress,
         const UaNodeId& variableAddress,
         const std::string& variableName,
         const UaVariant& value,
         UaStatus statusCode  )
{
    boost::lock_guard<decltype (m_lock)> lock (m_lock);
    m_pendingItems.emplace_back( "value", variableAddress.toString().toUtf8(), value.toString().toUtf8() );

}

void SociSqlArchiver::kill ()
{
    m_isRunning = false;
    m_archiverThread.join();
}

void SociSqlArchiver::archivingThread()
{
    while (m_isRunning)
    {
        usleep (5E6);
        boost::lock_guard<decltype (m_lock)> lock (m_lock);
        unsigned int numItems = m_pendingItems.size();
        LOG(Log::INF) << "queued items: " << numItems;
        if (numItems > 0)
        {
            for( const ArchivedItem& ai: m_pendingItems  )
            {
                m_session << "insert into quasar(address,value,time_stamp) values ('" << ai.address() << "','" << ai.value() << "',:ts)", soci::use(ai.timestamp());

            }
            m_pendingItems.clear();

        }


    }
}

UaStatus SociSqlArchiver::retrieveAssignment (
        const UaNodeId&   variableAddress,
        OpcUa_DateTime    timeFrom,
        OpcUa_DateTime    timeTo,
        unsigned int      maxValues,
        UaDataValues&     output )
{
    // first query how many elements we actually have
    return OpcUa_BadNotSupported;

}


}


