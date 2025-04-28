/*
ADVANCED
*/

create or replace sequence demo_seq start = 1 increment = 1;

create or replace table RAW_CREDITCO_SALES (
    ingestion_id int default demo_seq.nextval,
    ingested_at timestamp default current_timestamp(),
    data variant
);

insert into RAW_CREDITCO_SALES (data)
with create_data as (
    select parse_json('[{
          "id": "bad9cffb24fd560624859ae0c21f9135f5b005673dbd1d55ca75455cc0658b9a",
          "creditCardNumber": "30211992825494",
          "saleAmount": 302.2,
          "creditCardType": "diners-club-carte-blanche",
          "dateTime": "2022-04-06 12:24:38"
        }, {
          "id": "f70a7d8263289ad52bc948916d45325215396c6d89104056b5f141d4634add45",
          "creditCardNumber": "4405443328071522",
          "saleAmount": 279.17,
          "creditCardType": "visa-electron",
          "dateTime": "2023-02-16 10:21:46"
        }, {
          "id": "fb992d6a592ee80769f142fdaeb6334269f1f541fb13dce2e3704f050802b865",
          "creditCardNumber": "3579855610552574",
          "saleAmount": 445.47,
          "creditCardType": "jcb",
          "dateTime": "2022-06-01 23:16:18"
        }, {
          "id": "6dbdd40dfd3839e4da175301450ed500f6f78f6d4f1fdf854f350afb5d219bf9",
          "creditCardNumber": "50383603573914002",
          "saleAmount": 73.93,
          "creditCardType": "maestro",
          "dateTime": "2022-06-29 01:56:53"
        }, {
          "id": "e102d3a60180098cd3fcdbf5e08e461636ca4d21f413bd8b4bc650583f67efa0",
          "creditCardNumber": "5602228690840960",
          "saleAmount": 640.69,
          "creditCardType": "bankcard",
          "dateTime": "2022-10-16 22:37:15"
        }, {
          "id": "5291eb78808684cde42217736715cb29fb8ade849954536970d4dd3add48350d",
          "creditCardNumber": "5100144013918382",
          "saleAmount": 212.51,
          "creditCardType": "mastercard",
          "dateTime": "2023-01-18 21:32:30"
        }, {
          "id": "be52606565faf06a546ca1eaf1873f2cbf4d4863028bca598153727e2521584d",
          "creditCardNumber": "3566731438126971",
          "saleAmount": 633.21,
          "creditCardType": "jcb",
          "dateTime": "2022-05-22 22:17:52"
        }, {
          "id": "eeb62e335642e773c8c1ced13fe86c06e96d08633263ac6397967b91e6bb4483",
          "creditCardNumber": "3552372594286931",
          "saleAmount": 359.57,
          "creditCardType": "jcb",
          "dateTime": "2022-12-15 01:17:57"
        }, {
          "id": "ad187740d176791cd73aab91956c78974b720a6f4641a220fc899719b9bf7009",
          "creditCardNumber": "5038306986883304079",
          "saleAmount": 208.23,
          "creditCardType": "maestro",
          "dateTime": "2023-01-03 10:05:00"
        }, {
          "id": "61d49b03b011efc73e536560cc43f1d6c3a553434148376662bbc31063f883ac",
          "creditCardNumber": "3535351427856427",
          "saleAmount": 725.71,
          "creditCardType": "jcb",
          "dateTime": "2022-08-31 13:54:49"
        }]') as data
    union
    select parse_json('[{
          "id": "6929f37efca77ca1183de291878aeb01df7fe18ff933318e57e881855d1062fd",
          "creditCardNumber": "5508176932162294",
          "saleAmount": 104.31,
          "creditCardType": "mastercard",
          "dateTime": "2022-05-05 23:57:28"
        }, {
          "id": "bc91529cf8c5dc0177f4187a7ac291343cf15e91fc586df4a5c1022d8f864d98",
          "creditCardNumber": "5100170235305322",
          "saleAmount": 507.77,
          "creditCardType": "mastercard",
          "dateTime": "2022-11-17 15:32:08"
        }, {
          "id": "d6a5a2c3ceb5ffe288a16dd16ff55e243d4db535f14a699ce5a0df2e48d45f8d",
          "creditCardNumber": "3589433589169560",
          "saleAmount": 170.02,
          "creditCardType": "jcb",
          "dateTime": "2023-02-15 10:07:31"
        }, {
          "id": "b076f6b41e238c45e63c7fe4d1f66fdda1d157e8fc6af1e4505a4cc983cb6ec5",
          "creditCardNumber": "3555586629098303",
          "saleAmount": 501.04,
          "creditCardType": "jcb",
          "dateTime": "2023-01-04 01:34:06"
        }, {
          "id": "1f57f83f7b42aa8a986d4b01f2b996669a75639fc2d1fb6a01121c983ae8fc22",
          "creditCardNumber": "6706100376371730",
          "saleAmount": 129.57,
          "creditCardType": "laser",
          "dateTime": "2022-05-12 06:00:46"
        }, {
          "id": "6eda427326d9a3829621e73afb28ec924c8671b9b6f39f6aa5d85d7d570d6e24",
          "creditCardNumber": "5602247780904171",
          "saleAmount": 243.89,
          "creditCardType": "bankcard",
          "dateTime": "2022-07-17 15:16:38"
        }, {
          "id": "06db982c6bf7d306cf16af3cd01316e3aefb324dbbab077a07da14b7b4a993dc",
          "creditCardNumber": "3565858283414805",
          "saleAmount": 505.83,
          "creditCardType": "jcb",
          "dateTime": "2023-01-18 23:34:03"
        }, {
          "id": "8980dbde6642088242b8bf93e23d65d19c08737e058cdd4cd417837d512a1b5c",
          "creditCardNumber": "5641828030448807704",
          "saleAmount": 614.5,
          "creditCardType": "switch",
          "dateTime": "2022-10-06 00:06:08"
        }, {
          "id": "3e6d96ba37a3b92db0ebed1650da15ce078aa0d53a58dd814f242fb544573cb8",
          "creditCardNumber": "5108758559774396",
          "saleAmount": 729.55,
          "creditCardType": "mastercard",
          "dateTime": "2022-11-07 13:12:44"
        }, {
          "id": "f03665c549c08f30e5b319f72c744d11b81a5f4f24e8f9390f38c06f21144d10",
          "creditCardNumber": "3531925498468842",
          "saleAmount": 609.49,
          "creditCardType": "jcb",
          "dateTime": "2022-12-09 21:29:21"
        }]')
    union
    select parse_json(
        '[{
          "id": "ef9f2a1480245dcabe9a0028cf070c4c90e042a0912c12e75d94e905b2285130",
          "creditCardNumber": "3577254950335455",
          "saleAmount": 302.38,
          "creditCardType": "jcb",
          "dateTime": "2022-05-08 16:22:46"
        }, {
          "id": "40c139b8fa3ef091e6872e968d129bdea9301ae3d0584ab273250d0cae989a39",
          "creditCardNumber": "3554549240876508",
          "saleAmount": 147.71,
          "creditCardType": "jcb",
          "dateTime": "2023-04-02 12:06:35"
        }, {
          "id": "455264f770f0bda6b412e399bb38acf4c7ffd90b8e6344bb992f15dcbe7483bd",
          "creditCardNumber": "30266916616965",
          "saleAmount": 77.47,
          "creditCardType": "diners-club-carte-blanche",
          "dateTime": "2022-04-05 21:55:11"
        }, {
          "id": "b060f868e3e72b1989e3b2407193e4f8b62d84588024422fea0d32d7ff2c9295",
          "creditCardNumber": "633451185597375535",
          "saleAmount": 430.19,
          "creditCardType": "solo",
          "dateTime": "2022-11-16 09:32:35"
        }, {
          "id": "99df14a00fb6f97e4c1c376bd9b5dc9b115f7d9784a623ac551591bc7b17ca29",
          "creditCardNumber": "3534118906250666",
          "saleAmount": 253.83,
          "creditCardType": "jcb",
          "dateTime": "2022-12-07 22:57:35"
        }, {
          "id": "28f8f23c5c0b5bc4c9eda8abf96ffe9f36e177e8d8706064298a0f4f102b2f22",
          "creditCardNumber": "3575177399469647",
          "saleAmount": 564.36,
          "creditCardType": "jcb",
          "dateTime": "2023-02-11 20:01:45"
        }, {
          "id": "2ac5fe05ea1770a825e613e31e528f88af9c5f17e8d05fb2c2602e9fd9f49b47",
          "creditCardNumber": "6333020302312124279",
          "saleAmount": 291.37,
          "creditCardType": "switch",
          "dateTime": "2023-03-09 11:22:30"
        }, {
          "id": "67059c95cb922cd9146cbb7cb550861d4373833751daeaa829986093052a6eb3",
          "creditCardNumber": "3551078881931920",
          "saleAmount": 565.97,
          "creditCardType": "jcb",
          "dateTime": "2023-02-12 22:10:49"
        }, {
          "id": "65c230303222a2466ead2a15b2210b9333746b0697016d60124004bbe4ff7af4",
          "creditCardNumber": "3537111546909475",
          "saleAmount": 209.55,
          "creditCardType": "jcb",
          "dateTime": "2023-03-16 06:46:57"
        }, {
          "id": "3c3c0f2774b86361db8a5de484501788064f730a1cf12792ed7f4533a223ac6a",
          "creditCardNumber": "3557546242325275",
          "saleAmount": 310.73,
          "creditCardType": "jcb",
          "dateTime": "2022-08-23 11:26:17"
        }]')
    );

create or replace sequence demo_seq_2 start = 1 increment = 1;

create or replace table RAW_CREDITCO_SALES_ITEMS (
    ingestion_id int default demo_seq_2.nextval,
    ingested_at timestamp default current_timestamp(),
    data variant
);

insert into RAW_CREDITCO_SALES_ITEMS (data)
with create_data as (
    select parse_json('[
        {
            "id": "bad9cffb24fd560624859ae0c21f9135f5b005673dbd1d55ca75455cc0658b9a",
            "creditCardNumber": "30211992825494",
            "items": ["T-shirt", "Jeans", "Sneakers"],
            "dateTime": "2022-04-06 12:24:38"
        },
        {
            "id": "f70a7d8263289ad52bc948916d45325215396c6d89104056b5f141d4634add45",
            "creditCardNumber": "4405443328071522",
            "items": ["Dress", "Sandals"],
            "dateTime": "2023-02-16 10:21:46"
        },
        {
            "id": "fb992d6a592ee80769f142fdaeb6334269f1f541fb13dce2e3704f050802b865",
            "creditCardNumber": "3579855610552574",
            "items": ["Shirt", "Shorts", "Flip flops"],
            "dateTime": "2022-06-01 23:16:18"
        },
        {
            "id": "6dbdd40dfd3839e4da175301450ed500f6f78f6d4f1fdf854f350afb5d219bf9",
            "creditCardNumber": "50383603573914002",
            "items": ["Blouse", "Skirt", "Heels"],
            "dateTime": "2022-06-29 01:56:53"
        },
        {
            "id": "e102d3a60180098cd3fcdbf5e08e461636ca4d21f413bd8b4bc650583f67efa0",
            "creditCardNumber": "5602228690840960",
            "items": ["Sweater", "Pants", "Boots"],
            "dateTime": "2022-10-16 22:37:15"
        },
        {
            "id": "5291eb78808684cde42217736715cb29fb8ade849954536970d4dd3add48350d",
            "creditCardNumber": "5100144013918382",
            "items": ["Jacket", "Jeans"],
            "dateTime": "2023-01-18 21:32:30"
        },
        {
            "id": "be52606565faf06a546ca1eaf1873f2cbf4d4863028bca598153727e2521584d",
            "creditCardNumber": "3566731438126971",
            "items": ["Hoodie", "Leggings", "Sneakers"],
            "dateTime": "2022-05-22 22:17:52"
        },
        {
            "id": "eeb62e335642e773c8c1ced13fe86c06e96d08633263ac6397967b91e6bb4483",
            "creditCardNumber": "3552372594286931",
            "items": ["Polo shirt", "Shorts", "Sandals"],
            "dateTime": "2022-12-15 01:17:57"
        }, 
        {
            "id": "ad187740d176791cd73aab91956c78974b720a6f4641a220fc899719b9bf7009",
            "creditCardNumber": "5038306986883304079",
            "items": ["Blue Jeans", "White T-shirt", "Black Sneakers"],
            "dateTime": "2023-01-03 10:05:00"
        }, 
        {
            "id": "61d49b03b011efc73e536560cc43f1d6c3a553434148376662bbc31063f883ac",
            "creditCardNumber": "3535351427856427",
            "items": ["Striped Dress", "Silver Earrings", "Brown Sandals", "Leather Belt"],
            "dateTime": "2022-08-31 13:54:49"
        } 
    ]') as data
    union
    select parse_json('[
        {
            "id": "6929f37efca77ca1183de291878aeb01df7fe18ff933318e57e881855d1062fd",
            "creditCardNumber": "5508176932162294",
            "items": ["T-shirt", "Jeans"],
            "dateTime": "2022-05-05 23:57:28"
        },
        {
            "id": "bc91529cf8c5dc0177f4187a7ac291343cf15e91fc586df4a5c1022d8f864d98",
            "creditCardNumber": "5100170235305322",
            "items": ["T-shirt", "Jeans"],
            "dateTime": "2022-11-17 15:32:08"
        },
        {
            "id": "d6a5a2c3ceb5ffe288a16dd16ff55e243d4db535f14a699ce5a0df2e48d45f8d",
            "creditCardNumber": "3589433589169560",
            "items": ["Sweater", "Leggings", "Boots"],
            "dateTime": "2023-02-15 10:07:31"
        },
        {
            "id": "b076f6b41e238c45e63c7fe4d1f66fdda1d157e8fc6af1e4505a4cc983cb6ec5",
            "creditCardNumber": "3555586629098303",
            "items": ["Shirt", "Shorts"],
            "dateTime": "2023-01-04 01:34:06"
        },
        {
            "id": "1f57f83f7b42aa8a986d4b01f2b996669a75639fc2d1fb6a01121c983ae8fc22",
            "creditCardNumber": "6706100376371730",
            "items": ["Blouse", "Skirt", "Flats"],
            "dateTime": "2022-05-12 06:00:46"
        },
        {
            "id": "6eda427326d9a3829621e73afb28ec924c8671b9b6f39f6aa5d85d7d570d6e24",
            "creditCardNumber": "5602247780904171",
            "items": ["Hoodie", "Joggers"],
            "dateTime": "2022-07-17 15:16:38"
        },
        {
            "id": "06db982c6bf7d306cf16af3cd01316e3aefb324dbbab077a07da14b7b4a993dc",
            "creditCardNumber": "3565858283414805",
            "items": ["Polo Shirt", "Chinos"],
            "dateTime": "2023-01-18 23:34:03"
        },
        {
            "id": "8980dbde6642088242b8bf93e23d65d19c08737e058cdd4cd417837d512a1b5c",
            "creditCardNumber": "5641828030448807704",
            "items": ["Tank Top", "Capri Pants"],
            "dateTime": "2022-10-06 00:06:08"
        },
        {
            "id": "3e6d96ba37a3b92db0ebed1650da15ce078aa0d53a58dd814f242fb544573cb8",
            "creditCardNumber": "5108758559774396",
            "items": ["Jacket", "Dress Pants"],
            "dateTime": "2022-11-07 13:12:44"
        },
        {
            "id": "f03665c549c08f30e5b319f72c744d11b81a5f4f24e8f9390f38c06f21144d10",
            "creditCardNumber": "3531925498468842",
            "items": ["Cardigan", "Maxi Dress", "Sandals"],
            "dateTime": "2022-12-09 21:29:21"
        }
    ]')
    union
    select parse_json(
      '[
        {
            "id": "ef9f2a1480245dcabe9a0028cf070c4c90e042a0912c12e75d94e905b2285130",
            "creditCardNumber": "3577254950335455",
            "items": ["Sweatshirt", "Cargo Pants"],
            "dateTime": "2022-05-08 16:22:46"
        }, 
        {
            "id": "40c139b8fa3ef091e6872e968d129bdea9301ae3d0584ab273250d0cae989a39",
            "creditCardNumber": "3554549240876508",
            "items": ["Tunic", "Leggings", "Ankle Boots"],
            "dateTime": "2023-04-02 12:06:35"
        }, 
        {
            "id": "455264f770f0bda6b412e399bb38acf4c7ffd90b8e6344bb992f15dcbe7483bd",
            "creditCardNumber": "30266916616965",
            "items": ["Shawl", "Denim Skirt"],
            "dateTime": "2022-04-05 21:55:11"
        }, 
        {
            "id": "b060f868e3e72b1989e3b2407193e4f8b62d84588024422fea0d32d7ff2c9295",
            "creditCardNumber": "633451185597375535",
            "items": ["Pullover", "Sweatpants"],
            "dateTime": "2022-11-16 09:32:35"
        }, 
        {
            "id": "99df14a00fb6f97e4c1c376bd9b5dc9b115f7d9784a623ac551591bc7b17ca29",
            "creditCardNumber": "3534118906250666",
            "items": ["Blazer", "Slacks", "Loafers"],
            "dateTime": "2022-12-07 22:57:35"
        },
        {
            "id": "28f8f23c5c0b5bc4c9eda8abf96ffe9f36e177e8d8706064298a0f4f102b2f22",
            "creditCardNumber": "3575177399469647",
            "items": ["Button-up Shirt", "Khakis"],
            "dateTime": "2023-02-11 20:01:45"
        }, 
        {
            "id": "2ac5fe05ea1770a825e613e31e528f88af9c5f17e8d05fb2c2602e9fd9f49b47",
            "creditCardNumber": "6333020302312124279",
            "items": ["Vest", "Jeans", "Sneakers"],
            "dateTime": "2023-03-09 11:22:30"
        }, 
        {
            "id": "67059c95cb922cd9146cbb7cb550861d4373833751daeaa829986093052a6eb3",
            "creditCardNumber": "3551078881931920",
            "items": ["Crop Top", "Shorts"],
            "dateTime": "2023-02-12 22:10:49"
        }, 
        {
            "id": "65c230303222a2466ead2a15b2210b9333746b0697016d60124004bbe4ff7af4",
            "creditCardNumber": "3537111546909475",
            "items": ["Sweater Dress", "Tights", "Heels"],
            "dateTime": "2023-03-16 06:46:57"
        }, 
        {
            "id": "3c3c0f2774b86361db8a5de484501788064f730a1cf12792ed7f4533a223ac6a",
            "creditCardNumber": "3557546242325275",
            "items": ["Turtleneck", "Corduroy Pants"],
            "dateTime": "2022-08-23 11:26:17"
        }
    ]')
)
select * from create_data;