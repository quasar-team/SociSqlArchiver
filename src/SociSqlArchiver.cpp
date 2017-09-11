/*
 * SociSqlArchiver.cpp
 *
 *  Created on: 30 Aug 2017
 *      Author: pnikiel
 */

#include <ctime>

#include <boost/lexical_cast.hpp>

#include <uadatetime.h>
#include <uavariant.h>
#include <uadatavalue.h>

#include <ASUtils.h>
#include <LogIt.h>

#include <SociSqlArchiver.h>

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
        m_retrievalSession(sociAddress),
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
                m_session << "insert into quasar(address,value,time_stamp) values (:address,:value,:ts)",
                        soci::use(ai.address()),
                        soci::use(ai.value()),
                        soci::use(ai.timestamp());

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
    // TODO: try-catch for SoCi errors
    std::time_t ttFrom = UaDateTime (timeFrom).toTime_t();
    std::time_t ttTo = UaDateTime (timeTo).toTime_t();
    int count;

    std::string strVariableAddress ( variableAddress.toString().toUtf8() );

    std::vector<std::string> values (maxValues);
    std::vector<std::tm> time_stamps (maxValues);

    m_retrievalSession << "select value,time_stamp from quasar where address = :ad and time_stamp>:ts_from and time_stamp<:ts_to order by time_stamp",
            soci::use(strVariableAddress),
            soci::use(ttFrom),
            soci::use(ttTo),
            soci::into(values),
            soci::into(time_stamps);

    LOG(Log::INF) << "count was: " << count;

    output.resize( values.size() );

    for (int i=0; i<values.size();  ++i)
    {
        LOG(Log::INF) << "ts=" << time_stamps[i].tm_sec << " val=" << values[i];
        std::time_t tt = mktime( &time_stamps[i] );
        UaDateTime ts = UaDateTime::fromTime_t(tt);


        float v = boost::lexical_cast<float>(values[i]);

        UaDataValue dv ( v, OpcUa_Good, ts, ts );
        dv.copyTo(&output[i]);
        //output[i]
    }



    // first query how many elements we actually have
    return OpcUa_Good;

}


}


