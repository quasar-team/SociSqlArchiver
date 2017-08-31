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


namespace SociSqlArchiver
{

ArchivedItem::ArchivedItem( const std::string& attribute, const std::string& addr, const std::string& value ):
        m_attribute(attribute),
        m_address(addr),
        m_value(value)
{

        timespec ts;
        clock_gettime(CLOCK_REALTIME, &ts );
        int64_t timestamp = (int64_t)(ts.tv_sec) * (int64_t)1000000000 + (int64_t)(ts.tv_nsec);

        m_timestamp = boost::lexical_cast<std::string>( timestamp );

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
                m_session << "insert into quasar(address,value) values ('" << ai.address() << "','" << ai.value() << "')";
            }
            m_pendingItems.clear();

        }


    }
}


}


