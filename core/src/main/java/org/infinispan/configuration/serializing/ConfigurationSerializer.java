package org.infinispan.configuration.serializing;

import javax.xml.stream.XMLStreamException;

/**
 * @author Tristan Tarrant
 * @since 8.3
 */
public interface ConfigurationSerializer<T> {
   void serialize(XMLExtendedStreamWriter writer, T configuration) throws XMLStreamException;
}
