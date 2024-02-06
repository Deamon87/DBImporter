//
// Created by deamon on 02.04.18.
//

#include <stdint.h>
#include <algorithm>
#include <iostream>
#include <sstream>
#include <assert.h>
#include <cstring>
#include <cmath>
#include <iomanip>
#include "DB2Ver5.h"

using namespace WDC5;

static const inline bool checkDataIfNonZero(unsigned char *ptr, int length) {
    for (int i = 0; i < length; i++) {
        if (ptr[i] != 0)
            return false;
    }

    return true;
}

void DB2Ver5::process(HFileContent db2File, const std::string &fileName) {
    this->db2File = db2File;
    this->db2FileName = fileName;
    fileData = &(*this->db2File.get())[0];

    currentOffset = 0;
    bytesRead = 0;

    WDC5::db2_header header;
    readValue(header);
    readValue(headerContent);

    readValues(section_headers, headerContent->section_count);
    readValues(fields, headerContent->total_field_count);
    fieldInfoLength = headerContent->field_storage_info_size / sizeof(WDC3::field_storage_info);
    readValues(field_info, fieldInfoLength);


    int palleteDataRead = 0;
    if (headerContent->pallet_data_size > 0) {
        palleteDataArray.resize(headerContent->field_count);
        for (int i = 0; i < headerContent->field_count; i++) {
            if ((field_info[i].storage_type == WDC3::field_compression::field_compression_bitpacked_indexed) ||
                (field_info[i].storage_type == WDC3::field_compression::field_compression_bitpacked_indexed_array)) {

                for (int j = 0; j < field_info[i].additional_data_size / 4; j++) {
                    uint32_t value;
                    readValue(value);
                    palleteDataArray[i].push_back(value);
                    palleteDataRead++;
                }
            }
        }
    }

    assert(palleteDataRead*4 == headerContent->pallet_data_size);

//    readValues(pallet_data, headerContent->pallet_data_size);
    //Form hashtable for column

    if (headerContent->common_data_size > 0) {
        commonDataHashMap.resize(headerContent->field_count);
        for (int i = 0; i < headerContent->field_count; i++) {
            if (field_info[i].storage_type == WDC3::field_compression::field_compression_common_data) {
                int id;
                uint32_t value;
                for (int j = 0; j < field_info[i].additional_data_size / 8; j++) {
                    readValue(id);
                    readValue(value);

                    commonDataHashMap[i][id] = value;
                }
            }
        }
    }
//    readValues(common_data, );

    int size = 0;
    for (int i = 0; i < headerContent->section_count; i++) {
        auto &itemSectionHeader = section_headers[i];
        if (itemSectionHeader.tact_key_hash == 0) continue;

        uint32_t dataSize = 0;
        readValue(dataSize);

        std::vector<uint32_t> ids;
        ids.resize(dataSize);

        auto ptr = ids.data();
        readValues(ptr, dataSize);
    }

    //Read section
    sections.resize(headerContent->section_count);

    for (int i = 0; i < headerContent->section_count; i++) {
        auto &itemSectionHeader = section_headers[i];

        WDC3::section &section = sections[i];

//        if (itemSectionHeader.tact_key_hash != 0) break;

        assert(itemSectionHeader.file_offset == currentOffset);

        if (!headerContent->flags.isSparse) {
            // Normal records

            for (int j = 0; j < itemSectionHeader.record_count; j++) {
                WDC3::record_data recordData;
                readValues(recordData.data, headerContent->record_size);

                section.records.push_back(recordData);
            }
            readValues(section.string_data, itemSectionHeader.string_table_size);
        } else {
            // Offset map records -- these records have null-terminated strings inlined, and
            // since they are variable-length, they are pointed to by an array of 6-byte
            // offset+size pairs.
            readValues(section.variable_record_data, itemSectionHeader.offset_records_end - itemSectionHeader.file_offset);
        }

        if (itemSectionHeader.offset_records_end > 0) {
            assert(itemSectionHeader.offset_records_end == currentOffset);
        }

        readValues(section.id_list, itemSectionHeader.id_list_size / 4);

        if (itemSectionHeader.copy_table_count > 0) {
            readValues(section.copy_table, itemSectionHeader.copy_table_count);
        }

        if (headerContent->table_hash == 145293629)
            currentOffset+=itemSectionHeader.offset_map_id_count*4;

        readValues(section.offset_map, itemSectionHeader.offset_map_id_count);

        auto offsetBeforRelationshipData = currentOffset;
        if (itemSectionHeader.relationship_data_size > 0) {
            // In some tables, this relationship mapping replaced columns that were used
            // only as a lookup, such as the SpellID in SpellX* tables.
            readValue(section.relationship_map.num_entries);
            readValue(section.relationship_map.min_id);
            readValue(section.relationship_map.max_id);
            readValues(section.relationship_map.entries, section.relationship_map.num_entries);

            hasRelationShipField |= (section.relationship_map.num_entries > 0);

            for (int relationInd = 0; relationInd < section.relationship_map.num_entries; relationInd++) {
                const auto &entry = section.relationship_map.entries[relationInd];
                section.perRecordIndexRelation[entry.record_index] = entry.foreign_id;
            }

        }
        if (offsetBeforRelationshipData + itemSectionHeader.relationship_data_size != currentOffset) {
            currentOffset = offsetBeforRelationshipData + itemSectionHeader.relationship_data_size;
        }

        readValues(section.offset_map_id_list, itemSectionHeader.offset_map_id_count);

        if (itemSectionHeader.tact_key_hash != 0)
        {
            //Check if section was decrypted properly
            if (!headerContent->flags.isSparse) {
                section.isEncoded = checkDataIfNonZero(section.records[0].data, currentOffset - itemSectionHeader.file_offset);
            }
            else {
                section.isEncoded = checkDataIfNonZero(section.variable_record_data, currentOffset - itemSectionHeader.file_offset);
            }
        }
    }

    m_loaded = true;
}
