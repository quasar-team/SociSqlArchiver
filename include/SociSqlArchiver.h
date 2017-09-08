/*
 * SociSqlArchiver.h
 *
 *  Created on: 30 Aug 2017
 *      Author: pnikiel
 */

#ifndef SOCISQLARCHIVER_INCLUDE_SOCISQLARCHIVER_H_
#define SOCISQLARCHIVER_INCLUDE_SOCISQLARCHIVER_H_

#include <GenericArchiver.h>
#include <soci/soci.h>
#include <list>
#include <boost/thread.hpp>
#include <ctime>

namespace SociSqlArchiver
{

class ArchivedItem
{

public:
    ArchivedItem( const std::string& attribute, const std::string& addr, const std::string& value );

    std::string attribute() const { return m_attribute; }
    std::string address() const { return m_address; }
    std::string value () const { return m_value; }
    std::tm timestamp() const { return m_timestamp; }

private:
    std::string m_attribute;
    std::string m_address;
    std::string m_value;
    std::tm m_timestamp;
};

class SociSqlArchiver: public GenericArchiver::GenericArchiver
{
public:
    SociSqlArchiver (const std::string& sociAddress);
    ~SociSqlArchiver ();

    virtual void archiveAssignment (
            const UaNodeId& objectAddress,
            const UaNodeId& variableAddress,
            const std::string& variableName,
            const UaVariant& value,
            UaStatus statusCode  );

    virtual UaStatus retrieveAssignment (
            const UaNodeId&   variableAddress,
            OpcUa_DateTime    timeFrom,
            OpcUa_DateTime    timeTo,
            unsigned int      maxValues,
            UaDataValues&     output );

    virtual void kill ();

    void archivingThread ();

private:
    soci::session m_session;
    boost::thread m_archiverThread;
    bool m_isRunning;
    std::list<ArchivedItem> m_pendingItems;
    boost::mutex m_lock;

};

}


#endif /* SOCISQLARCHIVER_INCLUDE_SOCISQLARCHIVER_H_ */
