from lxml import etree
import re

import pandas as pd
from os import walk
from os.path import join
import json

def clean_tmp(tmp, level):
#  [subfonds] [series] [subseries] [file] [subfile] [item]
    if level == 'series':
        for k in list(tmp):
            if re.search("^(series|subseries|file|subfile|item)_", k):
                del tmp[k]
    if level == 'subseries':
        for k in list(tmp):
            if re.search("^(subseries|file|subfile|item)_", k):
                del tmp[k]
    elif level == 'file':
        for k in list(tmp):
            if re.search("^(file|subfile|item)_", k):
                del tmp[k]
    elif level == 'subfile':
        for k in list(tmp):
            if re.search("^(subfile|item)_", k):
                del tmp[k]
    return tmp

class RbxEadParser():

    def __init__(self, **kwargs):
        if 'ead_file' in kwargs:
            self.ead_file = kwargs.get('ead_file')
            self.tree = etree.parse(self.ead_file)
            self.metadata_eadheader = {}
            self.metadata_archdesc = {}
            self.metadata_dsc = []
            self.main_metadata = {}
            self.flat_metadata = []

    def get_persname(self):
        for element in self.tree.xpath("//persname"):
            print(f"{element.tag}: {element.text}")

    def parser(self):
        self.eadheader_parser()
        self.archdesc_parser()
        self.dsc_parser()
        for k, v in self.metadata_eadheader.items():
            k = f"eadheader_{k}"
            self.main_metadata[k] = v
        for k, v in self.metadata_archdesc.items():
            k = f"archdesc_{k}"
            self.main_metadata[k] = v
        for record in self.metadata_dsc:
            for k, v in self.main_metadata.items():
                record[k] = v
            self.flat_metadata.append(record)

    def eadheader_parser(self):
        eadheader = self.tree.xpath("/ead/eadheader")[0]
        self.metadata_eadheader['eadid'] = eadheader.xpath("//eadid")[0].text
        self.metadata_eadheader['titleproper'] = eadheader.xpath("//titleproper")[0].text
        self.metadata_eadheader['publication_date'] = eadheader.xpath("//publicationstmt/date")[0].text


    def archdesc_parser(self):
        self.metadata_archdesc = {}
        self.archdesc = self.tree.xpath("/ead/archdesc")
        for archdesc in self.archdesc:
            for archdesc_el in archdesc:
                if 'level' in archdesc_el.attrib:
                    self.metadata_archdesc['archdesc_level'] = archdesc_el.attrib['level']
                if archdesc_el.tag == 'did':
                    did = self.did_parser(archdesc_el)
                    for k, v in did.items():
                        self.metadata_archdesc[k] = v
                elif archdesc_el.tag == 'controlaccess':
                     controlaccess = self.controlaccess_parser(archdesc_el)
                     for k, v in controlaccess.items():
                         self.metadata_archdesc[k] = v
                elif archdesc_el.tag != 'dsc':
                    self.metadata_archdesc[archdesc_el.tag] = archdesc_el.text
                elif archdesc_el.tag == 'dsc':
                     self.dsc = archdesc_el

    def get_content(self, file_out):
        self.level = {}
        eadheader = self.tree.xpath("/ead/eadheader")[0]
        self.level['eadid'] = eadheader.xpath("//eadid")[0].text
        self.level['titleproper'] = eadheader.xpath("//titleproper")[0].text
        archdesc = self.tree.xpath("/ead/archdesc/did")[0]
        self.level['archdesc_did_unitid'] = archdesc.xpath("//unitid")[0].text
        self.level['archdesc_did_unittitle'] = archdesc.xpath("//unittitle")[0].text
        next_level = []
        if self.tree.xpath("/ead/archdesc/dsc/c"):
            for c0 in self.tree.xpath("/ead/archdesc/dsc/c"):
                if c0.xpath("c"):
                    next_level0 = []
                    for c1 in c0.xpath("c"):
                        if c1.xpath("c"):
                            next_level1 = []
                            for c2 in c1.xpath("c"):
                                if c2.xpath("c"):
                                    next_level2 = []
                                    for c3 in c2.xpath("c"):
                                        if c3.xpath("c"):
                                            next_level3 = []
                                            for c4 in c3.xpath("c"):
                                                next_level3.append(c4.xpath("did/unitid")[0].text)
                                            next_level2.append({c3.xpath("did/unitid")[0].text: next_level3})
                                        else:
                                            next_level2.append(c3.xpath("did/unitid")[0].text)
                                    next_level1.append({c2.xpath("did/unitid")[0].text: next_level2})
                                else:
                                    next_level1.append(c2.xpath("did/unitid")[0].text)
                            next_level0.append({c1.xpath("did/unitid")[0].text: next_level1})
                        else:
                            next_level0.append(c1.xpath("did/unitid")[0].text)
                    next_level.append({c0.xpath("did/unitid")[0].text: next_level0})
                else:
                    next_level.append(c0.xpath("did/unitid")[0].text)
        self.level['next_level'] = next_level

        out_file = open(file_out, "w")
        json.dump(self.level, out_file, indent = 4)
        out_file.close()

    def process_dsc_components(self, c, tmp = None):
        level = c.attrib['level']
        if tmp == None:
            tmp = {}
        else:
            tmp = clean_tmp(tmp, level)
        for el in c.xpath("*[not(self::c)]"):
            if el.tag == 'did':
                res = self.did_parser(el)
                tmp =  self.add2dict(res, tmp, k_prefix = level)
            elif el.tag == 'controlaccess':
                res = self.controlaccess_parser(el)
                tmp =  self.add2dict(res, tmp, k_prefix = level)
            elif el.tag[:3] == 'dao':
                res = self.dao_parser(el)
                tmp =  self.add2dict(res, tmp, k_prefix = level)
        if c.xpath("*[(self::c)]"):
            for el in c.xpath("*[(self::c)]"):
                self.process_dsc_components(el, tmp = tmp)
        else:
            self.metadata_dsc.append(tmp.copy())

    def dsc_parser(self):
        for c in self.dsc:
            self.process_dsc_components(c, tmp = None)

    def dao_parser(self, element):
        metadata = {}
        if element.tag == 'dao':
            if 'href' in element.attrib:
                metadata['file_first'] = element.attrib['href']
            if 'audience' in element.attrib:
                metadata['audience'] = element.attrib['audience']
        elif element.tag == 'daogrp':
            for el in element:
                if el.tag == 'daoloc':
                    if ( ('role' in el.attrib) and ('href' in el.attrib) ):
                        if el.attrib['role'] == 'image:first':
                            metadata['file_first'] = el.attrib['href']
                        elif el.attrib['role'] == 'image:last':
                            metadata['file_last'] = el.attrib['href']
                    if 'audience' in element.attrib:
                        metadata['audience'] = element.attrib['audience']
        metadata_res = {}
        return self.add2dict(metadata, metadata_res, k_prefix = "dao")

    def did_parser(self, element):
        # cas du langmaterial à traiter
        metadata = {}
        for el in element:
            if el.text:
                text = el.text.strip()
                if ( (el.tag == 'unitdate') and 'normal' in el.attrib.keys() ):
                    newtag = "unidate_normal"
                    tag = el.tag
                    if tag in metadata:
                        metadata[tag].append(text)
                    else:
                        metadata[tag] = []
                        metadata[tag].append(text)
                    if newtag in metadata:
                        metadata[newtag].append(el.attrib['normal'])
                    else:
                        metadata[newtag] = []
                        metadata[newtag].append(el.attrib['normal'])
                else:
                    tag = el.tag
                    if tag in metadata:
                        metadata[tag].append(text)
                    else:
                        metadata[tag] = []
                        metadata[tag].append(text)
            elif el.tag == 'langmaterial':
                for e in el:
                    if e.tag =='language':
                        metadata['langmaterial_language'] = [f"{e.text}|{e.attrib['langcode']}"]
        for k in metadata:
            metadata[k] = ";".join(metadata[k])
            metadata[k] = metadata[k].replace("<p>", "").replace("</p>", "")
        metadata_res = {}
        return self.add2dict(metadata, metadata_res, k_prefix = "did")

    def controlaccess_parser(self, element):
        metadata = {}
        for el in element:
            if el.text:
                text = el.text.strip()
                if ( (el.tag == 'subject') and 'source' in el.attrib.keys() ):
                    tag = f"{el.tag}_{el.attrib['source']}"
                    if tag in metadata:
                        metadata[tag].append(text)
                    else:
                        metadata[tag] = []
                        metadata[tag].append(text)
                elif el.tag == 'geogname':
                    tag = el.tag
                    if tag in metadata:
                        metadata[tag].append(text)
                    else:
                        metadata[tag] = []
                        metadata[tag].append(text)
                    if 'coord' in el.attrib.keys():
                        tag = 'geogname_coord'
                        if tag in metadata:
                            metadata[tag].append(el.attrib['coord'])
                        else:
                            metadata[tag] = []
                            metadata[tag].append(el.attrib['coord'])
                else:
                    tag = el.tag
                    if tag in metadata:
                        metadata[tag].append(text)
                    else:
                        metadata[tag] = []
                        metadata[tag].append(text)
        for k in metadata:
            metadata[k] = ";".join(metadata[k])
            metadata[k] = metadata[k].replace("<p>", "").replace("</p>", "")
        metadata_res = {}
        return self.add2dict(metadata, metadata_res, k_prefix = "controlaccess")

    def add2dict(self, res, metadata_temp, k_prefix):
        for k, v in res.items():
            if k_prefix:
                k = f"{k_prefix}_{k}"
            metadata_temp[k] = v
        return metadata_temp

if __name__ == "__main__":
    data_folder = 'data'
    inventaires_folder = 'inventaires_ead'
    # ead_file = 'FR595129901_MED_07.xml'
    ead_file = 'FR595129901_MUS_03.xml'
    ead_file_path = join(data_folder, inventaires_folder, ead_file)

    eadparser = RbxEadParser(ead_file=ead_file_path)
    eadparser.parser()
    #print(eadparser.flat_metadata)
    #
    res_file = f"{ead_file[:-4]}.xlsx"
    res_folder = "results"
    res_file_path = join(res_folder, res_file)
    result = pd.DataFrame(eadparser.flat_metadata)
#    result = result.drop_duplicates()
    result.to_excel(res_file_path, index=False)
