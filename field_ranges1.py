# Field ranges for DAPERMIT
dapermit_field_ranges = {
    "record_id": (0, 2),
    "da_permit_number": (2, 9),
    "da_permit_sequence_number": (9, 11),
    "da_permit_county_code": (11, 14),
    "da_permit_lease_name": (14, 46),
    "da_permit_district": (46, 48),
    "da_permit_well_number": (48, 54),
    "da_permit_total_depth": (54, 59),
    "da_permit_operator_number": (59, 65),
    "da_type_application": (65, 67),
    "da_other_explanation": (67, 97),
    "da_address_unique_number": (97, 103),
    "da_zip_code_prefix": (103, 108),
    "da_zip_code_suffix": (108, 112),
    "da_fiche_set_number": (112, 118),
    "da_onshore_county": (118, 121),
    "da_received_century": (121, 123),
    "da_received_year": (123, 125),
    "da_received_month": (125, 127),
    "da_received_day": (127, 129),
    "da_permit_issued_century": (129, 131),
    "da_permit_issued_year": (131, 133),
    "da_permit_issued_month": (133, 135),
    "da_permit_issued_day": (135, 137),
    "da_permit_amended_century": (137, 139),
    "da_permit_amended_year": (139, 141),
    "da_permit_amended_month": (141, 143),
    "da_permit_amended_day": (143, 145),
    "da_permit_extended_century": (145, 147),
    "da_permit_extended_year": (147, 149),
    "da_permit_extended_month": (149, 151),
    "da_permit_extended_day": (151, 153),
    "da_permit_spud_century": (153, 155),
    "da_permit_spud_year": (155, 157),
    "da_permit_spud_month": (157, 159),
    "da_permit_spud_day": (159, 161),
    "da_permit_surface_casing_century": (161, 163),
    "da_permit_surface_casing_year": (163, 165),
    "da_permit_surface_casing_month": (165, 167),
    "da_permit_surface_casing_day": (167, 169),
    "da_well_status": (169, 170),
    "da_well_status_date_century": (170, 172),
    "da_well_status_date_year": (172, 174),
    "da_well_status_date_month": (174, 176),
    "da_well_status_date_day": (176, 178),
    "da_permit_expired_century": (178, 180),
    "da_permit_expired_year": (180, 182),
    "da_permit_expired_month": (182, 184),
    "da_permit_expired_day": (184, 186),
    "da_permit_cancelled_century": (186, 188),
    "da_permit_cancelled_year": (188, 190),
    "da_permit_cancelled_month": (190, 192),
    "da_permit_cancelled_day": (192, 194),
    "da_cancellation_reason": (194, 224),
    "da_p12_filed_flag": (224, 225),
    "da_substandard_acreage_flag": (225, 226),
    "da_rule_36_flag": (226, 227),
    "da_h9_flag": (227, 228),
    "da_rule_37_case_number": (229, 236),
    "da_rule_38_docket_number": (236, 243),
    "da_location_formation_flag": (243, 244),
    "da_old_surface_location": (244, 296),
    "da_new_surface_location": (296, 326),
    "da_surface_acres": (326, 334),
    "da_surface_miles_from_city": (334, 340),
    "da_surface_direction_from_city": (340, 346),
    "da_surface_nearest_city": (346, 359),
    "da_surface_lease_distance": (359, 388),
    "da_old_lease_distance_format": (388, 401),
    "da_surface_survey_distance": (401, 430),
    "da_old_survey_distance_format": (430, 443),
    "da_nearest_well": (443, 471),
    "da_nearest_well_format_flag": (471, 472),
    "da_final_update": (472, 478),
    "da_cancelled_flag": (478, 479),
    "da_spud_in_flag": (479, 480),
    "da_directional_well_flag": (480, 481),
    "da_sidetrack_well_flag": (481, 482),
    "da_moved_indicator": (482, 483),
    "da_permit_conv_issued_date": (485, 493),
    "da_rule_37_granted_code": (493, 494),
    "da_horizontal_well_flag": (494, 495),
    "da_duplicate_permit_flag": (495, 496),
    "da_nearest_lease_line": (496, 503),
    "api_number": (503, 511),  # Assuming this goes to 511
}


dafldbhl_field_ranges = {
    "record_id": (0, 2),
    "da_fld_bhl_section": (2, 10),
    "da_fld_bhl_block": (10, 20),
    "da_fld_bhl_abstract": (20, 26),
    "da_fld_bhl_survey": (26, 81),
    "da_fld_bhl_acres": (81, 83),
    "da_fld_bhl_nearest_well": (83, 111),
    "da_fld_bhl_lease_feet_1": (111, 119),
    "da_fld_bhl_lease_direction_1": (119, 132),
    "da_fld_bhl_lease_feet_2": (132, 138),
    "da_fld_bhl_lease_direction_2": (138, 151),
    "da_fld_bhl_survey_feet_1": (151, 157),
    "da_fld_bhl_survey_direction_1": (157, 170),
    "da_fld_bhl_survey_feet_2": (170, 176),
    "da_fld_bhl_survey_direction_2": (176, 189),
    "da_fld_bhl_county": (189, 202),
    "da_fld_bhl_pntrt_dist_1": (202, 210),
    "da_fld_bhl_pntrt_dir_1": (210, 223),
    "da_fld_bhl_pntrt_dist_2": (223, 229),
    "da_fld_bhl_pntrt_dir_2": (229, 242),
    "da_filler": (242, 260)
}
daroot_field_ranges = {
    "record_id": (0, 2),  # RRC-TAPE-RECORD-ID
    "da_status_number": (2, 9),  # DA-STATUS-NUMBER
    "da_status_sequence_number": (9, 11),  # DA-STATUS-SEQUENCE-NUMBER
    "da_county_code": (11, 14),  # DA-COUNTY-CODE
    "da_lease_name": (14, 46),  # DA-LEASE-NAME
    "da_district": (46, 48),  # DA-DISTRICT
    "da_operator_number": (48, 54),  # DA-OPERATOR-NUMBER
    "da_converted_date": (54, 58),  # DA-CONVERTED-DATE (COMP format, may need special handling)
    "da_app_rcvd_century": (58, 60),  # DA-APP-RCVD-CENTURY
    "da_app_rcvd_year": (60, 62),  # DA-APP-RCVD-YEAR
    "da_app_rcvd_month": (62, 64),  # DA-APP-RCVD-MONTH
    "da_app_rcvd_day": (64, 66),  # DA-APP-RCVD-DAY
    "da_operator_name": (66, 98),  # DA-OPERATOR-NAME
    "filler": (98, 99),  # FILLER
    "da_hb1407_problem_flag": (99, 100),  # DA-HB1407-PROBLEM-FLAG
    "da_status_of_app_flag": (100, 101),  # DA-STATUS-OF-APP-FLAG
    "da_problem_flags": (101, 113),  # DA-PROBLEM-FLAGS (various problem flags)
    "da_permit": (113, 120),  # DA-PERMIT
    "da_issue_century": (120, 122),  # DA-ISSUE-CENTURY
    "da_issue_year": (122, 124),  # DA-ISSUE-YEAR
    "da_issue_month": (124, 126),  # DA-ISSUE-MONTH
    "da_issue_day": (126, 128),  # DA-ISSUE-DAY
    "da_withdrawn_century": (128, 130),  # DA-WITHDRAWN-CENTURY
    "da_withdrawn_year": (130, 132),  # DA-WITHDRAWN-YEAR
    "da_withdrawn_month": (132, 134),  # DA-WITHDRAWN-MONTH
    "da_withdrawn_day": (134, 136),  # DA-WITHDRAWN-DAY
    "da_walkthrough_flag": (136, 137),  # DA-WALKTHROUGH-FLAG
    "da_other_problem_text": (137, 157),  # DA-OTHER-PROBLEM-TEXT
    "da_well_number": (157, 163),  # DA-WELL-NUMBER
    "da_built_from_old_master_flag": (163, 164),  # DA-BUILT-FROM-OLD-MASTER-FLAG
    "da_status_renumbered_to": (164, 173),  # DA-STATUS-RENUMBERED-TO
    "da_status_renumbered_from": (173, 182),  # DA-STATUS-RENUMBERED-FROM
    "da_application_returned_flag": (182, 183),  # DA-APPLICATION-RETURNED-FLAG
    "da_ecap_filing_flag": (183, 184),  # DA-ECAP-FILING-FLAG
    "filler_2": (184, 187),  # FILLER
    "rrc_tape_filler": (187, 512)  # RRC-TAPE-FILLER
}



dadafield_field_ranges = {
    "record_id": (0, 2),
    "da_field_number": (2, 8),
    "da_field_application_well_code": (8, 9),
    "da_field_completion_well_code": (9, 10),
    "da_field_completion_code": (10, 11),
    "da_field_transfer_code": (11, 12),
    "da_field_validation_century": (12, 14),
    "da_field_validation_year": (14, 16),
    "da_field_validation_month": (16, 18),
    "da_field_validation_day": (18, 20),
    "da_field_completion_century": (20, 22),
    "da_field_completion_year": (22, 24),
    "da_field_completion_month": (24, 26),
    "da_field_completion_day": (26, 28),
    "da_field_rule37_flag": (28, 29),
    "da_field_rule38_flag": (29, 30),
    "rrc_tape_filler": (30, 490)  # Adjusted range based on the provided structure
}

dafldspc_field_ranges = {
    "record_id": (0, 2),
    "da_field_district": (2, 4),
    "da_field_lease_name": (4, 36),
    "da_field_total_depth": (36, 41),
    "da_field_well_number": (41, 47),
    "da_field_acres": (47, 53),
    "filler": (53, 70),
    "rrc_tape_filler": (70, 523)  # Adjusted range based on the provided structure
}

dacanres_field_ranges = {
    "record_id": (0, 2),
    "da_can_restr_key": (2, 4),
    "da_can_restr_type": (4, 6),
    "da_can_restr_remark": (6, 41),
    "da_can_restr_flag": (480, 481)
}

dacanfld_field_ranges = {
    "record_id": (0, 2),
    "da_can_restr_fld_number": (2, 10),  # Adjusted range
    "filler": (10, 15),  # Adjusted range
    "rrc_tape_filler": (15, 510)  # Adjusted range
}


dafreres_field_ranges = {
    "record_id": (0, 2),
    "da_free_restr_key": (2, 4),  # Adjusted range
    "da_free_restr_type": (4, 6),  # Adjusted range
    "da_free_restr_remark": (6, 76),  # Adjusted range
    "da_free_restr_flag": (76, 77),  # Adjusted range
    "filler": (77, 87),  # Adjusted range
    "rrc_tape_filler": (87, 510)  # Adjusted range
}

dafrefld_field_ranges = {
    "record_id": (0, 2),
    "da_free_restr_fld_number": (2, 10),  # Adjusted based on the file format provided
    "filler": (10, 15),
    "rrc_tape_filler": (15, 510)  # Adjusted range based on the provided structure
}

dapmtbhl_field_ranges = {
    "record_id": (0, 2),
    "da_pmt_bhl_section": (2, 10),
    "da_pmt_bhl_block": (10, 20),
    "da_pmt_bhl_abstract": (20, 26),
    "da_pmt_bhl_survey": (26, 81),
    "da_pmt_bhl_acres": (81, 88),
    "da_pmt_bhl_nearest_well": (89, 117),
    "da_pmt_bhl_lease_feet_1": (117, 124),
    "da_pmt_bhl_lease_direction_1": (124, 137),
    "da_pmt_bhl_lease_feet_2": (137, 144),
    "da_pmt_bhl_lease_direction_2": (144, 157),
    "da_pmt_bhl_survey_feet_1": (157, 164),
    "da_pmt_bhl_survey_direction_1": (164, 177),
    "da_pmt_bhl_survey_feet_2": (177, 184),
    "da_pmt_bhl_survey_direction_2": (184, 197),
    "da_pmt_bhl_county": (197, 210),
    "da_pmt_bhl_pntrt_dist_1": (210, 217),
    "da_pmt_bhl_pntrt_dir_1": (217, 230),
    "da_pmt_bhl_pntrt_dist_2": (230, 237),
    "da_pmt_bhl_pntrt_dir_2": (237, 250),
    "rrc_tape_filler": (250, 260)
}

daaltadd_field_ranges = {
    "record_id": (0, 2),
    "da_alt_address_key": (2, 4),
    "da_alt_address_line_1": (4, 37),
    "da_alternate_address_line_2": (37, 72),
    "rrc_tape_filler": (72, 510)
}

daremark_field_ranges = {
    "record_id": (0, 2),
    "da_remark_sequence_number": (2, 5),
    "da_remark_file_century": (5, 7),
    "da_remark_file_year": (7, 9),
    "da_remark_file_month": (9, 11),
    "da_remark_file_day": (11, 13),
    "da_remark_line": (13, 83),
    "filler": (83, 93),
    "rrc_tape_filler": (93, 510)
}

dacheck_field_ranges = {
    "record_id": (0, 2),
    "da_check_register_key": (2, 10),
    "da_check_register_date_century": (10, 12),
    "da_check_register_date_year": (12, 14),
    "da_check_register_date_month": (14, 16),
    "da_check_register_date_day": (16, 18),
    "da_check_register_number": (18, 26),
    "filler": (26, 36),
    "rrc_tape_filler": (36, 518)
}
